package database

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"

	_ "github.com/mattn/go-sqlite3"
)

var DataDir = "/var/lib/pewpaw"

func init() {
	configDir, err := os.UserConfigDir()
	if err == nil {
		DataDir = filepath.Join(configDir, "pewpaw")
	}
}

const (
	KatanaDB        = "katana.db"
	UserstoreDB     = "userstore.db"
	MsgstoreDB      = "msgstore.db"
	MsgstoreBackup  = "msgstore.1.db"
)

type Manager struct {
	Katana    *sql.DB
	Userstore *sql.DB
	Msgstore  *sql.DB
	Msgstore1 *sql.DB
}

func InitAll() (*Manager, error) {
	if err := os.MkdirAll(DataDir, 0700); err != nil {
		return nil, fmt.Errorf("create data dir: %w", err)
	}

	katana, err := initDB(filepath.Join(DataDir, KatanaDB), katanaSchema)
	if err != nil {
		return nil, fmt.Errorf("katana db: %w", err)
	}

	userstore, err := initDB(filepath.Join(DataDir, UserstoreDB), userstoreSchema)
	if err != nil {
		return nil, fmt.Errorf("userstore db: %w", err)
	}

	msgstore, err := initDB(filepath.Join(DataDir, MsgstoreDB), msgstoreSchema)
	if err != nil {
		return nil, fmt.Errorf("msgstore db: %w", err)
	}

	msgstore1, err := initDB(filepath.Join(DataDir, MsgstoreBackup), msgstoreSchema)
	if err != nil {
		return nil, fmt.Errorf("msgstore.1 db: %w", err)
	}

	return &Manager{Katana: katana, Userstore: userstore, Msgstore: msgstore, Msgstore1: msgstore1}, nil
}

func initDB(path, schema string) (*sql.DB, error) {
	db, err := sql.Open("sqlite3", path+"?_journal_mode=WAL&_foreign_keys=on")
	if err != nil {
		return nil, err
	}
	if _, err := db.Exec(schema); err != nil {
		return nil, err
	}
	return db, nil
}

func (m *Manager) Close() {
	if m.Katana != nil {
		m.Katana.Close()
	}
	if m.Userstore != nil {
		m.Userstore.Close()
	}
	if m.Msgstore != nil {
		m.Msgstore.Close()
	}
	if m.Msgstore1 != nil {
		m.Msgstore1.Close()
	}
}

const katanaSchema = `
CREATE TABLE IF NOT EXISTS sessions (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    jid         TEXT    NOT NULL UNIQUE,
    device_id   INTEGER NOT NULL DEFAULT 0,
    token       BLOB    NOT NULL,
    created_at  DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at  DATETIME DEFAULT CURRENT_TIMESTAMP
);
`

const userstoreSchema = `
CREATE TABLE IF NOT EXISTS contacts (
    jid         TEXT    PRIMARY KEY,
    name        TEXT    NOT NULL DEFAULT '',
    push_name   TEXT    NOT NULL DEFAULT '',
    verified    INTEGER NOT NULL DEFAULT 0
);
`

const msgstoreSchema = `
CREATE TABLE IF NOT EXISTS chats (
    jid             TEXT    PRIMARY KEY,
    display_name    TEXT    NOT NULL DEFAULT '',
    unread_count    INTEGER NOT NULL DEFAULT 0,
    last_message    TEXT,
    last_timestamp  INTEGER NOT NULL DEFAULT 0,
    archived        INTEGER NOT NULL DEFAULT 0,
    muted_until     INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS messages (
    id              TEXT    PRIMARY KEY,
    chat_jid        TEXT    NOT NULL,
    sender_jid      TEXT    NOT NULL,
    content         TEXT    NOT NULL DEFAULT '',
    message_type    TEXT    NOT NULL DEFAULT 'text',
    timestamp       INTEGER NOT NULL,
    is_from_me      INTEGER NOT NULL DEFAULT 0,
    is_read         INTEGER NOT NULL DEFAULT 0,
    media_path      TEXT,
    quoted_msg_id   TEXT,
    FOREIGN KEY (chat_jid) REFERENCES chats(jid)
);

CREATE INDEX IF NOT EXISTS idx_messages_chat ON messages(chat_jid, timestamp);
CREATE INDEX IF NOT EXISTS idx_messages_timestamp ON messages(timestamp);
`
