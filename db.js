// db.js - SQLite3 simple, compatible Windows/macOS/Linux
const sqlite3 = require('sqlite3').verbose();

// La base sera créée dans ce fichier local
const db = new sqlite3.Database('hebrew-duo.db');

// Création des tables si nécessaires
db.serialize(() => {
  db.run(`CREATE TABLE IF NOT EXISTS users (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    email TEXT UNIQUE NOT NULL,
    password_hash TEXT NOT NULL,
    display_name TEXT NOT NULL,
    role TEXT NOT NULL DEFAULT 'user',
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
  )`);

  db.run(`CREATE TABLE IF NOT EXISTS themes (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    parent_id INTEGER,
    user_id INTEGER
  )`);

  db.run(`CREATE TABLE IF NOT EXISTS words (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    hebrew TEXT NOT NULL,
    transliteration TEXT,
    french TEXT NOT NULL,
    theme_id INTEGER,
    difficulty INTEGER DEFAULT 1,
    active INTEGER DEFAULT 1,
    user_id INTEGER,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
  )`);

  db.run(`CREATE TABLE IF NOT EXISTS progress (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER NOT NULL,
    word_id INTEGER NOT NULL,
    strength INTEGER DEFAULT 0,
    success_count INTEGER DEFAULT 0,
    fail_count INTEGER DEFAULT 0,
    last_seen DATETIME
  )`);

  db.run(`CREATE TABLE IF NOT EXISTS favorites (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER NOT NULL,
    word_id INTEGER NOT NULL,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(user_id, word_id)
  )`);
});

// Ajout de colonnes manquantes sur une base existante (ignore si déjà là)
const safeAlter = (sql) => {
  db.run(sql, (err) => {
    if (err && !/duplicate column/.test(err.message)) {
      console.error('Alter error:', err.message);
    }
  });
};

safeAlter('ALTER TABLE words ADD COLUMN active INTEGER DEFAULT 1');
safeAlter('ALTER TABLE words ADD COLUMN user_id INTEGER');
safeAlter('ALTER TABLE themes ADD COLUMN user_id INTEGER');

module.exports = db;
