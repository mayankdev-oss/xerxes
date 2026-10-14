# Kinetic Backup Vault (`kinetic-backup`)

A lightweight, fault-tolerant file backup vault built with **Go (1.22+)** and **MongoDB Atlas**, styled after the **TemplateMo 631 Kinetic** dark brutalist aesthetic. It provides a virtual `/root` directory tree where non-conflicting folders can be created and raw files up to **25 MB** per `POST` request can be backed up, streamed, downloaded, or wiped.

---

## 1. System Architecture & Cluster Communication Flow

Communication between the Browser Client, the Go HTTP Server, and the MongoDB Atlas Cluster occurs in two distinct layers: **HTTP/1.1 (or HTTP/2) Multipart & JSON** at the application edge, and the **MongoDB Wire Protocol (`OP_MSG` over TLS/TCP)** across the Atlas cluster.

```text
+-------------------+       HTTP / REST        +--------------------+    MongoDB Wire Protocol   +-------------------------+
|                   |  (JSON & Multipart/Form) |                    |      (BSON over TLS)       |   MongoDB Atlas Cluster |
|  Browser Client   | -----------------------> |   Go HTTP Server   | -------------------------> |    (Primary Replica)    |
| (Kinetic Web UI)  | <----------------------- | (http.MaxBytes 25M)| <------------------------- |                         |
+-------------------+    Binary Stream / JSON  +--------------------+     GridFS 255KB Chunks    +-------------------------+
                                                                                                    |  ├── folders
                                                                                                    |  ├── files_meta
                                                                                                    |  ├── fs.files
                                                                                                    |  └── fs.chunks
```

### How Instructions Travel Over the Cluster
1. **Connection Pooling & SRV Discovery:** When the Go server starts (`mongo.Connect`), it resolves the `mongodb+srv://` DNS record to discover all replica set nodes in your MongoDB Atlas cluster. It establishes a persistent TLS-encrypted TCP connection pool.
2. **Write Concern & Primary Routing:** Every state-changing instruction (`POST` folder creation, `POST` file upload, `DELETE` folder/DB) is serialized by the Go driver into binary **BSON** messages (`OP_MSG`) and routed strictly to the **Primary node** of the Atlas Replica Set. With `retryWrites=true&w=majority`, Atlas replicates the write to a majority of secondary nodes before acknowledging success back to the Go server.
3. **Streaming Backpressure (25 MB Limit):** During a `POST /api/upload`, the Go server wraps the incoming TCP stream with `http.MaxBytesReader(w, r.Body, 25<<20)`. Instead of loading the entire 25 MB file into RAM at once, Go buffers the multipart stream and pipes it in 255 KB segments directly over the wire to Atlas. If the incoming stream exceeds `26,214,400` bytes by even 1 byte, the stream is severed immediately and returns `413 Request Entity Too Large`.

---

## 2. How MongoDB Atlas Actually Stores the Raw File (GridFS Internals)

MongoDB has a strict **16 MB maximum BSON document size limit**. Because this application supports payloads up to **25 MB**, storing a raw file inside a single MongoDB document would fail for files between 16 MB and 25 MB.

To solve this, the application uses **MongoDB GridFS**, which splits the raw file across two specialized collections (`fs.files` and `fs.chunks`) alongside our custom index collections (`folders` and `files_meta`).

### Storage Breakdown of a 25 MB File Upload

When you upload `backup.zip` (25 MB) into `/root/backup_1`[cite: 2], four collections interact inside `kinetic_backup_db`:

#### A. `folders` Collection (Virtual Directory Registry)
Enforces unique directory names inside `/root` via a unique BSON index on `{ "name": 1 }`[cite: 2].
```json
{
  "_id": ObjectId("670c1a..."),
  "name": "backup_1",
  "root_path": "/root/backup_1",
  "created_at": ISODate("2026-10-14T00:00:00Z")
}
```

#### B. `fs.files` Collection (GridFS Parent File Record)
Created automatically by the GridFS driver to hold stream metadata and chunk sizing:
```json
{
  "_id": ObjectId("670c1b99f1..."),
  "length": NumberLong(26214400),
  "chunkSize": 261120,
  "uploadDate": ISODate("2026-10-14T00:01:00Z"),
  "filename": "backup.zip",
  "metadata": {
    "folder": "backup_1",
    "content_type": "application/zip"
  }
}
```

#### C. `fs.chunks` Collection (The Raw Binary Payload)
GridFS slices the raw incoming file stream into **255 KB (`261,120` byte)** slices. A 25 MB (`26,214,400` byte) file is broken into **101 separate BSON documents** (`n: 0` through `n: 100`). Each chunk stores its raw bytes inside a BSON `BinData` (subtype `0x00`) field:
```json
{
  "_id": ObjectId("670c1b99f2..."),
  "files_id": ObjectId("670c1b99f1..."), // Foreign key pointing to fs.files._id
  "n": 0,                                // Sequence number (0, 1, 2 ... 100)
  "data": BinData(0, "<255 KB of raw binary bytes>")
}
```
* **During Upload (`POST`):** The Go `gridfs.Bucket.UploadFromStream` reader pulls 255 KB at a time from the HTTP request and inserts sequential chunk documents (`n: 0..N`) into `fs.chunks`.
* **During Download (`GET`):** The Go server queries `fs.chunks` by `files_id` sorted by `n: 1`, reassembling the binary slices in order and piping them directly to the HTTP response writer.

#### D. `files_meta` Collection (Application Fast-Lookup & Conflict Guard)
Maps the virtual folder path to the `GridFSID` and enforces a compound unique index on `{ "folder": 1, "filename": 1 }` so two files with the same name cannot collide in the same folder:
```json
{
  "_id": ObjectId("670c1b99f9..."),
  "gridfs_id": ObjectId("670c1b99f1..."), // Links to fs.files._id
  "folder": "backup_1",
  "filename": "backup.zip",
  "size": 26214400,
  "content_type": "application/zip",
  "uploaded_at": ISODate("2026-10-14T00:01:00Z")
}
```

---

## 3. API Endpoint Mapping

| Method | Endpoint | Purpose | Request Payload | Cluster Instruction Executed |
| :--- | :--- | :--- | :--- | :--- |
| **`GET`** | `/api/state` | Fetch `/root` folder tree & file list | *None* | Runs `find({})` on `folders` and `files_meta` collections. |
| **`POST`** | `/api/folders` | Create a subfolder inside `/root`[cite: 2] | `{"name": "backup_1"}` | Validates regex `^[a-zA-Z0-9_-]{1,64}$`; runs `insertOne` on `folders`. Returns `409 Conflict` on duplicate key. |
| **`POST`** | `/api/upload` | Upload file (Max 25 MB) to `/root/{folder}`[cite: 2] | `multipart/form-data` (`folder`, `file`) | Verifies folder exists; streams 255 KB chunks to `fs.chunks` & `fs.files`; inserts record in `files_meta`. Rolls back GridFS chunks on conflict. |
| **`GET`** | `/api/files/{folder}/{filename}` | Read (`inline`) or Download (`?download=1`) file | Query param `?download=1` (optional) | Looks up `gridfs_id` in `files_meta`; streams ordered `BinData` chunks from `fs.chunks` over HTTP. |
| **`DELETE`** | `/api/folders/{folder}` | **Delete Type 2:** Wipe specific folder & its backups[cite: 2] | *None* | Finds all `files_meta` in `{folder}`; calls `bucket.Delete(gridfs_id)` to purge `fs.files` & `fs.chunks`; deletes `files_meta` & `folders` records. |
| **`DELETE`** | `/api/db` | **Delete Type 1:** Drop entire backup database[cite: 2] | *None* | Sends `{ dropDatabase: 1 }` command to Atlas primary, wiping all collections & chunks, then rebuilds unique indexes. |

---

## 4. Edge Cases Handled

* **Payload > 25 MB:** Blocked at the HTTP stream layer via `http.MaxBytesReader` returning `413 Request Entity Too Large` before exhausting server memory.
* **Empty (0 Byte) Files:** Rejected with `400 Bad Request` prior to opening a GridFS stream.
* **Directory Traversal & Injection:** Folder names are restricted to `^[a-zA-Z0-9_-]{1,64}$` and filenames are sanitized with `filepath.Base()`, neutralizing `../` or `/` path injection.
* **Folder & File Name Collisions:** Enforced at the database level via MongoDB unique indexes (`E11000 duplicate key error` mapped to `409 Conflict`).
* **Orphaned GridFS Chunks:** If metadata insertion fails after streaming to GridFS, the server immediately executes `bucket.Delete(gridFSID)` to roll back the chunks.
* **Post-Drop Index Loss:** Dropping a database in MongoDB also destroys its indexes. The `DELETE /api/db` handler automatically re-runs `ensureIndexes()` immediately after dropping the DB so subsequent requests remain protected against name conflicts.

---

## 5. Setup, Testing & Execution

### Prerequisites
* **Go** 1.22 or higher
* **MongoDB Atlas** cluster URI

### Run Automated Edge-Case Tests
```bash
export MONGO_URI="mongodb+srv://<user>:<password>@<cluster>.mongodb.net/?retryWrites=true&w=majority"
go test -v ./...
```

### Start the Server
```bash
export MONGO_URI="mongodb+srv://<user>:<password>@<cluster>.mongodb.net/?retryWrites=true&w=majority"
export PORT=8080
go run main.go
```
Open `http://localhost:8080` in your browser[cite: 2].