package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/joho/godotenv"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/gridfs"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const (
	MaxUploadSize = 25 << 20 // 25 MB (26,214,400 bytes)
	RootDir       = "/root"
	DBName        = "kinetic_backup_db"
)

var validFolderRegex = regexp.MustCompile(`^[a-zA-Z0-9_-]{1,64}$`)

type App struct {
	Client *mongo.Client
	DB     *mongo.Database
}

type Folder struct {
	ID        primitive.ObjectID `bson:"_id,omitempty" json:"id"`
	Name      string             `bson:"name" json:"name"`
	RootPath  string             `bson:"root_path" json:"root_path"`
	CreatedAt time.Time          `bson:"created_at" json:"created_at"`
}

type FileMeta struct {
	ID          primitive.ObjectID `bson:"_id,omitempty" json:"id"`
	GridFSID    primitive.ObjectID `bson:"gridfs_id" json:"gridfs_id"`
	Folder      string             `bson:"folder" json:"folder"`
	Filename    string             `bson:"filename" json:"filename"`
	Size        int64              `bson:"size" json:"size"`
	ContentType string             `bson:"content_type" json:"content_type"`
	UploadedAt  time.Time          `bson:"uploaded_at" json:"uploaded_at"`
}

func NewApp(ctx context.Context, uri, dbName string) (*App, error) {
	clientOpts := options.Client().ApplyURI(uri)
	client, err := mongo.Connect(ctx, clientOpts)
	if err != nil {
		return nil, err
	}
	if err := client.Ping(ctx, nil); err != nil {
		return nil, err
	}

	db := client.Database(dbName)
	app := &App{Client: client, DB: db}
	if err := app.ensureIndexes(ctx); err != nil {
		return nil, err
	}
	return app, nil
}

// Ensure unique indexes to strictly avoid folder & file name conflicts
func (a *App) ensureIndexes(ctx context.Context) error {
	folderIdx := mongo.IndexModel{
		Keys:    bson.D{{Key: "name", Value: 1}},
		Options: options.Index().SetUnique(true),
	}
	if _, err := a.DB.Collection("folders").Indexes().CreateOne(ctx, folderIdx); err != nil {
		return err
	}

	fileIdx := mongo.IndexModel{
		Keys:    bson.D{{Key: "folder", Value: 1}, {Key: "filename", Value: 1}},
		Options: options.Index().SetUnique(true),
	}
	_, err := a.DB.Collection("files_meta").Indexes().CreateOne(ctx, fileIdx)
	return err
}

func writeJSON(w http.ResponseWriter, status int, data any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(data)
}

func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]string{"error": message})
}

func (a *App) Routes() http.Handler {
	mux := http.NewServeMux()

	// API Endpoints
	mux.HandleFunc("GET /api/state", a.handleGetState)
	mux.HandleFunc("POST /api/folders", a.handleCreateFolder)
	mux.HandleFunc("POST /api/upload", a.handleUploadFile)
	mux.HandleFunc("GET /api/files/{folder}/{filename}", a.handleGetFile)
	mux.HandleFunc("DELETE /api/folders/{folder}", a.handleWipeFolder)
	mux.HandleFunc("DELETE /api/db", a.handleDropDB)

	// Static Frontend
	fs := http.FileServer(http.Dir("./static"))
	mux.Handle("/", fs)

	return mux
}

// GET /api/state - Lists all folders in /root and their backed-up files
func (a *App) handleGetState(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()

	var folders []Folder
	cur, err := a.DB.Collection("folders").Find(ctx, bson.M{})
	if err != nil {
		writeError(w, http.StatusInternalServerError, "Failed to read folders")
		return
	}
	_ = cur.All(ctx, &folders)

	var files []FileMeta
	fCur, err := a.DB.Collection("files_meta").Find(ctx, bson.M{})
	if err != nil {
		writeError(w, http.StatusInternalServerError, "Failed to read files")
		return
	}
	_ = fCur.All(ctx, &files)

	if folders == nil {
		folders = []Folder{}
	}
	if files == nil {
		files = []FileMeta{}
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"root":    RootDir,
		"folders": folders,
		"files":   files,
	})
}

// POST /api/folders - Creates a folder inside /root and prevents name conflicts
func (a *App) handleCreateFolder(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Name string `json:"name"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "Invalid JSON payload")
		return
	}

	folderName := strings.TrimSpace(req.Name)
	if !validFolderRegex.MatchString(folderName) {
		writeError(w, http.StatusBadRequest, "Invalid folder name: use 1-64 alphanumeric characters, dashes, or underscores")
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()

	folder := Folder{
		Name:      folderName,
		RootPath:  fmt.Sprintf("%s/%s", RootDir, folderName),
		CreatedAt: time.Now().UTC(),
	}

	_, err := a.DB.Collection("folders").InsertOne(ctx, folder)
	if err != nil {
		if mongo.IsDuplicateKeyError(err) {
			writeError(w, http.StatusConflict, fmt.Sprintf("Folder '%s' already exists in %s", folderName, RootDir))
			return
		}
		writeError(w, http.StatusInternalServerError, "Database error creating folder")
		return
	}

	writeJSON(w, http.StatusCreated, folder)
}

// POST /api/upload - Max 25MB payload uploaded to a specific folder in /root
func (a *App) handleUploadFile(w http.ResponseWriter, r *http.Request) {
	// Enforce strict 25MB limit on request body
	r.Body = http.MaxBytesReader(w, r.Body, MaxUploadSize)

	if err := r.ParseMultipartForm(10 << 20); err != nil {
		var maxBytesErr *http.MaxBytesError
		if errors.As(err, &maxBytesErr) {
			writeError(w, http.StatusRequestEntityTooLarge, "Payload exceeds maximum allowed limit of 25MB")
			return
		}
		writeError(w, http.StatusBadRequest, "Invalid multipart form data or file exceeds 25MB")
		return
	}

	folderName := strings.TrimSpace(r.FormValue("folder"))
	if !validFolderRegex.MatchString(folderName) {
		writeError(w, http.StatusBadRequest, "Valid target folder in /root is required")
		return
	}

	file, header, err := r.FormFile("file")
	if err != nil {
		writeError(w, http.StatusBadRequest, "Missing file in form field 'file'")
		return
	}
	defer file.Close()

	if header.Size == 0 {
		writeError(w, http.StatusBadRequest, "Cannot upload an empty (0 byte) file")
		return
	}
	if header.Size > MaxUploadSize {
		writeError(w, http.StatusRequestEntityTooLarge, "File size exceeds 25MB limit")
		return
	}

	cleanFilename := filepath.Base(strings.TrimSpace(header.Filename))
	if cleanFilename == "." || cleanFilename == "/" || cleanFilename == "" {
		writeError(w, http.StatusBadRequest, "Invalid filename")
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 60*time.Second)
	defer cancel()

	// Verify target folder exists in /root
	err = a.DB.Collection("folders").FindOne(ctx, bson.M{"name": folderName}).Err()
	if errors.Is(err, mongo.ErrNoDocuments) {
		writeError(w, http.StatusNotFound, fmt.Sprintf("Target folder '%s/%s' does not exist", RootDir, folderName))
		return
	} else if err != nil {
		writeError(w, http.StatusInternalServerError, "Error verifying folder")
		return
	}

	// Check if file already exists in this folder to avoid orphaned GridFS chunks
	count, err := a.DB.Collection("files_meta").CountDocuments(ctx, bson.M{
		"folder":   folderName,
		"filename": cleanFilename,
	})
	if err != nil {
		writeError(w, http.StatusInternalServerError, "Error checking existing files")
		return
	}
	if count > 0 {
		writeError(w, http.StatusConflict, fmt.Sprintf("File '%s' already exists in %s/%s", cleanFilename, RootDir, folderName))
		return
	}

	bucket, err := gridfs.NewBucket(a.DB)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "Failed to initialize GridFS bucket")
		return
	}

	contentType := header.Header.Get("Content-Type")
	if contentType == "" {
		contentType = "application/octet-stream"
	}

	uploadOpts := options.GridFSUpload().SetMetadata(bson.M{
		"folder":       folderName,
		"content_type": contentType,
	})

	gridFSID, err := bucket.UploadFromStream(cleanFilename, file, uploadOpts)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "Failed to stream backup to MongoDB GridFS")
		return
	}

	meta := FileMeta{
		GridFSID:    gridFSID,
		Folder:      folderName,
		Filename:    cleanFilename,
		Size:        header.Size,
		ContentType: contentType,
		UploadedAt:  time.Now().UTC(),
	}

	_, err = a.DB.Collection("files_meta").InsertOne(ctx, meta)
	if err != nil {
		_ = bucket.Delete(gridFSID) // Rollback GridFS upload on metadata conflict
		writeError(w, http.StatusConflict, "File conflict detected during upload")
		return
	}

	writeJSON(w, http.StatusCreated, meta)
}

// GET /api/files/{folder}/{filename} - Read or download file from backup
func (a *App) handleGetFile(w http.ResponseWriter, r *http.Request) {
	folderName := r.PathValue("folder")
	filename := r.PathValue("filename")

	ctx, cancel := context.WithTimeout(r.Context(), 60*time.Second)
	defer cancel()

	var meta FileMeta
	err := a.DB.Collection("files_meta").FindOne(ctx, bson.M{
		"folder":   folderName,
		"filename": filename,
	}).Decode(&meta)

	if errors.Is(err, mongo.ErrNoDocuments) {
		writeError(w, http.StatusNotFound, "File not found in backup")
		return
	} else if err != nil {
		writeError(w, http.StatusInternalServerError, "Database query failed")
		return
	}

	bucket, err := gridfs.NewBucket(a.DB)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "Failed to open GridFS")
		return
	}

	var buf bytes.Buffer
	if _, err := bucket.DownloadToStream(meta.GridFSID, &buf); err != nil {
		writeError(w, http.StatusInternalServerError, "Failed to read file stream from GridFS")
		return
	}

	disposition := "inline"
	if r.URL.Query().Get("download") == "1" {
		disposition = "attachment"
	}

	w.Header().Set("Content-Type", meta.ContentType)
	w.Header().Set("Content-Disposition", fmt.Sprintf(`%s; filename="%s"`, disposition, meta.Filename))
	w.Header().Set("Content-Length", fmt.Sprintf("%d", buf.Len()))
	_, _ = io.Copy(w, &buf)
}

// DELETE /api/folders/{folder} - Type 2 Delete: Wipe specific folder in /root and all its files
func (a *App) handleWipeFolder(w http.ResponseWriter, r *http.Request) {
	folderName := r.PathValue("folder")

	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	// Find all files in this folder to delete their GridFS chunks
	cur, err := a.DB.Collection("files_meta").Find(ctx, bson.M{"folder": folderName})
	if err != nil {
		writeError(w, http.StatusInternalServerError, "Failed to lookup folder files")
		return
	}
	var files []FileMeta
	_ = cur.All(ctx, &files)

	bucket, err := gridfs.NewBucket(a.DB)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "GridFS initialization error")
		return
	}

	for _, f := range files {
		_ = bucket.Delete(f.GridFSID)
	}

	_, _ = a.DB.Collection("files_meta").DeleteMany(ctx, bson.M{"folder": folderName})

	res, err := a.DB.Collection("folders").DeleteOne(ctx, bson.M{"name": folderName})
	if err != nil {
		writeError(w, http.StatusInternalServerError, "Failed to delete folder record")
		return
	}
	if res.DeletedCount == 0 {
		writeError(w, http.StatusNotFound, fmt.Sprintf("Folder '%s' not found in %s", folderName, RootDir))
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"message":       fmt.Sprintf("Wiped folder %s/%s and %d backed-up files", RootDir, folderName, len(files)),
		"deleted_files": len(files),
	})
}

// DELETE /api/db - Type 1 Delete: Drop the entire database and re-initialize indexes
func (a *App) handleDropDB(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	if err := a.DB.Drop(ctx); err != nil {
		writeError(w, http.StatusInternalServerError, "Failed to drop database")
		return
	}

	// Re-apply unique indexes so subsequent folder creations stay conflict-free
	_ = a.ensureIndexes(ctx)

	writeJSON(w, http.StatusOK, map[string]string{
		"message": "Entire MongoDB backup database dropped and reset.",
	})
}

func main() {
	// Automatically loads variables from .env if present
	_ = godotenv.Load()

	uri := os.Getenv("MONGO_URI")
	if uri == "" {
		log.Fatal("MONGO_URI is required in .env or environment variables (e.g., mongodb+srv://user:pass@cluster.mongodb.net/)")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	app, err := NewApp(ctx, uri, DBName)
	if err != nil {
		log.Fatalf("Failed to initialize application: %v", err)
	}

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	log.Printf("Kinetic Backup Vault running on http://localhost:%s", port)
	if err := http.ListenAndServe(":"+port, app.Routes()); err != nil {
		log.Fatal(err)
	}
}
