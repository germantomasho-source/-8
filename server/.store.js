'use strict';
// Хранилище комнат. Сейчас: память + JSON-файл (атомарная запись, с задержкой).
// Интерфейс (get/set/delete/all/flush) намеренно маленький: позже его можно заменить
// на Redis/БД, не трогая server.js (методы станут async).
const fs = require('fs');
const path = require('path');

class FileStore {
  constructor(file) {
    this.file = file;
    this.rooms = new Map();
    this.timer = null;
    try {
      const data = JSON.parse(fs.readFileSync(file, 'utf8'));
      for (const r of data.rooms || []) this.rooms.set(r.code, r);
    } catch (e) { if (e.code !== 'ENOENT') console.error('store: не удалось прочитать', file, e.message); }
  }
  get(code) { return this.rooms.get(code); }
  set(room) { this.rooms.set(room.code, room); this._dirty(); }
  delete(code) { this.rooms.delete(code); this._dirty(); }
  all() { return [...this.rooms.values()]; }
  touch() { this._dirty(); }
  _dirty() { if (!this.timer) this.timer = setTimeout(() => this.flush(), 300); }
  flush() {
    clearTimeout(this.timer); this.timer = null;
    try {
      fs.mkdirSync(path.dirname(this.file), { recursive: true });
      const tmp = this.file + '.tmp';
      fs.writeFileSync(tmp, JSON.stringify({ v: 1, rooms: this.all() }));
      fs.renameSync(tmp, this.file);
      return true;
    } catch (e) { console.error('store: ошибка записи', e.message); return false; }
  }
}
module.exports = { FileStore };
