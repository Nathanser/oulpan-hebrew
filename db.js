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
    user_id INTEGER,
    active INTEGER DEFAULT 1,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
  )`);

  db.run(`CREATE TABLE IF NOT EXISTS theme_levels (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    theme_id INTEGER NOT NULL,
    name TEXT NOT NULL,
    level_order INTEGER DEFAULT 1,
    active INTEGER DEFAULT 1
  )`);

  db.run(`CREATE TABLE IF NOT EXISTS words (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    hebrew TEXT NOT NULL,
    transliteration TEXT,
    french TEXT NOT NULL,
    theme_id INTEGER,
    level_id INTEGER,
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

  db.run(`CREATE TABLE IF NOT EXISTS sets (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    user_id INTEGER NOT NULL,
    active INTEGER DEFAULT 1,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
  )`);

  db.run(`CREATE TABLE IF NOT EXISTS cards (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    set_id INTEGER NOT NULL,
    french TEXT NOT NULL,
    hebrew TEXT NOT NULL,
    transliteration TEXT,
    position INTEGER DEFAULT 1,
    active INTEGER DEFAULT 1,
    favorite INTEGER DEFAULT 0,
    memorized INTEGER DEFAULT 0,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
  )`);

  db.run(`CREATE TABLE IF NOT EXISTS card_progress (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER NOT NULL,
    card_id INTEGER NOT NULL,
    success_count INTEGER DEFAULT 0,
    fail_count INTEGER DEFAULT 0,
    last_seen DATETIME
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
safeAlter('ALTER TABLE words ADD COLUMN level_id INTEGER');
safeAlter('ALTER TABLE themes ADD COLUMN active INTEGER DEFAULT 1');
safeAlter('ALTER TABLE theme_levels ADD COLUMN active INTEGER DEFAULT 1');
safeAlter('ALTER TABLE sets ADD COLUMN active INTEGER DEFAULT 1');
safeAlter('ALTER TABLE cards ADD COLUMN active INTEGER DEFAULT 1');
safeAlter('ALTER TABLE cards ADD COLUMN position INTEGER DEFAULT 1');
safeAlter('ALTER TABLE cards ADD COLUMN favorite INTEGER DEFAULT 0');
safeAlter('ALTER TABLE cards ADD COLUMN memorized INTEGER DEFAULT 0');
safeAlter('CREATE TABLE IF NOT EXISTS card_progress (id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER NOT NULL, card_id INTEGER NOT NULL, success_count INTEGER DEFAULT 0, fail_count INTEGER DEFAULT 0, last_seen DATETIME)');
safeAlter('ALTER TABLE themes ADD COLUMN created_at DATETIME');
safeAlter('CREATE TABLE IF NOT EXISTS user_word_overrides (id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER NOT NULL, word_id INTEGER NOT NULL, active INTEGER NOT NULL DEFAULT 1, UNIQUE(user_id, word_id))');
safeAlter('CREATE TABLE IF NOT EXISTS set_shares (id INTEGER PRIMARY KEY AUTOINCREMENT, set_id INTEGER NOT NULL, user_id INTEGER NOT NULL, UNIQUE(set_id, user_id))');
safeAlter('CREATE TABLE IF NOT EXISTS card_overrides (id INTEGER PRIMARY KEY AUTOINCREMENT, card_id INTEGER NOT NULL, user_id INTEGER NOT NULL, active INTEGER DEFAULT NULL, favorite INTEGER DEFAULT NULL, memorized INTEGER DEFAULT NULL, UNIQUE(card_id, user_id))');
safeAlter('CREATE TABLE IF NOT EXISTS user_set_overrides (id INTEGER PRIMARY KEY AUTOINCREMENT, set_id INTEGER NOT NULL, user_id INTEGER NOT NULL, active INTEGER NOT NULL DEFAULT 1, UNIQUE(set_id, user_id))');
safeAlter('CREATE TABLE IF NOT EXISTS user_theme_overrides (id INTEGER PRIMARY KEY AUTOINCREMENT, theme_id INTEGER NOT NULL, user_id INTEGER NOT NULL, active INTEGER NOT NULL DEFAULT 1, UNIQUE(theme_id, user_id))');
safeAlter('ALTER TABLE set_shares ADD COLUMN can_edit INTEGER DEFAULT 0');
db.run('UPDATE themes SET created_at = CURRENT_TIMESTAMP WHERE created_at IS NULL', err => {
  if (err && !/no such column/i.test(err.message)) {
    console.error('Alter fill error:', err.message);
  }
});

module.exports = db;
