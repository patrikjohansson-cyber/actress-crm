const { DatabaseSync } = require('node:sqlite');
const path = require('path');

const db = new DatabaseSync(process.env.DB_PATH || path.join(__dirname, 'crm.db'));

db.exec(`
  CREATE TABLE IF NOT EXISTS contacts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    role TEXT,
    organization TEXT,
    email TEXT,
    phone TEXT,
    notes TEXT,
    priority INTEGER DEFAULT 2,
    tags TEXT DEFAULT '[]',
    photo_url TEXT,
    created_at TEXT DEFAULT (datetime('now')),
    updated_at TEXT DEFAULT (datetime('now'))
  );

  CREATE TABLE IF NOT EXISTS interactions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    contact_id INTEGER NOT NULL REFERENCES contacts(id) ON DELETE CASCADE,
    date TEXT NOT NULL,
    type TEXT,
    summary TEXT,
    cv_sent INTEGER DEFAULT 0,
    direction TEXT DEFAULT 'outgoing',
    created_at TEXT DEFAULT (datetime('now'))
  );

  CREATE TABLE IF NOT EXISTS projects (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    title TEXT NOT NULL,
    type TEXT,
    organization TEXT,
    status TEXT DEFAULT 'kommande',
    start_date TEXT,
    end_date TEXT,
    notes TEXT,
    own_work INTEGER DEFAULT 0,
    created_at TEXT DEFAULT (datetime('now')),
    updated_at TEXT DEFAULT (datetime('now'))
  );

  CREATE TABLE IF NOT EXISTS contact_projects (
    contact_id INTEGER NOT NULL REFERENCES contacts(id) ON DELETE CASCADE,
    project_id INTEGER NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    role_in_project TEXT,
    PRIMARY KEY (contact_id, project_id)
  );

  CREATE TABLE IF NOT EXISTS jonna_profile (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL,
    updated_at TEXT DEFAULT (datetime('now'))
  );

  CREATE TABLE IF NOT EXISTS contact_photos (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    contact_id INTEGER NOT NULL REFERENCES contacts(id) ON DELETE CASCADE,
    url TEXT NOT NULL,
    source TEXT DEFAULT 'manual',
    is_primary INTEGER DEFAULT 0,
    created_at TEXT DEFAULT (datetime('now'))
  );

  CREATE TABLE IF NOT EXISTS job_listings (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    title TEXT,
    organization TEXT,
    description TEXT,
    url TEXT,
    deadline TEXT,
    interesting_score INTEGER DEFAULT 0,
    interesting_reason TEXT,
    known_contacts TEXT DEFAULT '[]',
    saved INTEGER DEFAULT 0,
    found_at TEXT DEFAULT (datetime('now'))
  );

  CREATE TABLE IF NOT EXISTS stipend_findings (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    person_name TEXT,
    organization TEXT,
    year TEXT,
    description TEXT,
    url TEXT,
    matched_contact INTEGER REFERENCES contacts(id),
    saved INTEGER DEFAULT 0,
    feedback INTEGER DEFAULT 0,
    found_at TEXT DEFAULT (datetime('now'))
  );

  CREATE TABLE IF NOT EXISTS grant_calls (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    title TEXT,
    organization TEXT,
    description TEXT,
    url TEXT,
    deadline TEXT,
    amount TEXT,
    match_reason TEXT,
    saved INTEGER DEFAULT 0,
    feedback INTEGER DEFAULT 0,
    found_at TEXT DEFAULT (datetime('now'))
  );

  CREATE TABLE IF NOT EXISTS emails (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    message_id TEXT UNIQUE,
    from_address TEXT,
    to_address TEXT,
    subject TEXT,
    body_text TEXT,
    received_at TEXT,
    matched_contact INTEGER REFERENCES contacts(id),
    parsed INTEGER DEFAULT 0,
    interaction_id INTEGER REFERENCES interactions(id),
    created_at TEXT DEFAULT (datetime('now'))
  );
`);

// Index för snabb uppslagning av matchade e-poster vid kontaktborttagning
db.exec(`CREATE INDEX IF NOT EXISTS idx_emails_matched_contact ON emails(matched_contact)`);

// Applications table
db.exec(`
  CREATE TABLE IF NOT EXISTS applications (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    opportunity_type TEXT NOT NULL DEFAULT 'manual',
    opportunity_id INTEGER,
    opportunity_title TEXT,
    opportunity_organization TEXT,
    opportunity_deadline TEXT,
    opportunity_url TEXT,
    document_type TEXT DEFAULT 'personal_letter',
    status TEXT DEFAULT 'draft',
    generated_text TEXT,
    edited_text TEXT,
    ai_analysis TEXT,
    generation_prompt TEXT,
    notes TEXT,
    sent_at TEXT,
    created_at TEXT DEFAULT (datetime('now')),
    updated_at TEXT DEFAULT (datetime('now'))
  );
`);

// Organizations table
db.exec(`
  CREATE TABLE IF NOT EXISTS organizations (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    domain TEXT,
    type TEXT,
    notes TEXT,
    generic_emails TEXT DEFAULT '[]',
    created_at TEXT DEFAULT (datetime('now')),
    updated_at TEXT DEFAULT (datetime('now'))
  );

  CREATE TABLE IF NOT EXISTS contact_organizations (
    contact_id INTEGER NOT NULL REFERENCES contacts(id) ON DELETE CASCADE,
    organization_id INTEGER NOT NULL REFERENCES organizations(id) ON DELETE CASCADE,
    PRIMARY KEY (contact_id, organization_id)
  );
`);

// Migrations for future columns
try { db.exec(`ALTER TABLE contacts ADD COLUMN photo_url TEXT`); } catch {}
try { db.exec(`ALTER TABLE contacts ADD COLUMN enrichment_data TEXT`); } catch {}
try { db.exec(`ALTER TABLE contacts ADD COLUMN enriched_at TEXT`); } catch {}
try { db.exec(`ALTER TABLE contacts ADD COLUMN education TEXT DEFAULT '[]'`); } catch {}
try { db.exec(`ALTER TABLE contacts ADD COLUMN extra_emails TEXT DEFAULT '[]'`); } catch {}
try { db.exec(`ALTER TABLE contacts ADD COLUMN website TEXT`); } catch {}
try { db.exec(`ALTER TABLE job_listings ADD COLUMN feedback INTEGER DEFAULT 0`); } catch {}
try { db.exec(`ALTER TABLE job_listings ADD COLUMN opportunity_subtype TEXT DEFAULT 'job'`); } catch {}
try { db.exec(`ALTER TABLE contacts ADD COLUMN industry_star INTEGER DEFAULT 0`); } catch {}
try { db.exec(`ALTER TABLE organizations ADD COLUMN ai_summary TEXT`); } catch {}
try { db.exec(`ALTER TABLE organizations ADD COLUMN email TEXT`); } catch {}
try { db.exec(`ALTER TABLE projects ADD COLUMN ai_data TEXT`); } catch {}
try { db.exec(`ALTER TABLE projects ADD COLUMN description TEXT`); } catch {}
try { db.exec(`ALTER TABLE projects ADD COLUMN jonna_role TEXT`); } catch {}
try { db.exec(`ALTER TABLE projects ADD COLUMN jonna_notes TEXT`); } catch {}
try { db.exec(`ALTER TABLE projects ADD COLUMN director TEXT`); } catch {}
try { db.exec(`ALTER TABLE projects ADD COLUMN venue TEXT`); } catch {}
try { db.exec(`ALTER TABLE projects ADD COLUMN num_performances TEXT`); } catch {}
try { db.exec(`ALTER TABLE projects ADD COLUMN press_data TEXT`); } catch {}
try { db.exec(`ALTER TABLE projects ADD COLUMN links TEXT DEFAULT '[]'`); } catch {}
try { db.exec(`ALTER TABLE projects ADD COLUMN context_info TEXT`); } catch {}
try { db.exec(`ALTER TABLE contacts ADD COLUMN roles TEXT DEFAULT '[]'`); } catch {}
try { db.exec(`ALTER TABLE contacts ADD COLUMN extra_phones TEXT DEFAULT '[]'`); } catch {}
try { db.exec(`ALTER TABLE emails ADD COLUMN matched_organization INTEGER REFERENCES organizations(id)`); } catch {}
db.exec(`CREATE INDEX IF NOT EXISTS idx_emails_matched_organization ON emails(matched_organization)`);
// Migrera befintlig role → roles-array för kontakter som saknar det
try {
  db.exec(`UPDATE contacts SET roles = json_array(role) WHERE role IS NOT NULL AND role != '' AND (roles IS NULL OR roles = '[]')`);
} catch {}

module.exports = {
  // ── Contacts ──────────────────────────────────────────────
  getContacts({ search, role, priority } = {}) {
    const where = [];
    const params = [];
    if (search) { where.push(`(c.name LIKE ? OR c.organization LIKE ? OR c.roles LIKE ?)`); params.push(`%${search}%`, `%${search}%`, `%${search}%`); }
    if (role)     { where.push(`(c.role = ? OR c.roles LIKE ?)`); params.push(role, `%"${role}"%`); }
    if (priority) { where.push('c.priority = ?'); params.push(Number(priority)); }
    let q = `SELECT c.*,
      (SELECT GROUP_CONCAT(o.name, ', ') FROM organizations o
       JOIN contact_organizations co ON o.id = co.organization_id
       WHERE co.contact_id = c.id) as org_names,
      (SELECT GROUP_CONCAT(p.title, '|||') FROM projects p
       JOIN contact_projects cp ON p.id = cp.project_id
       WHERE cp.contact_id = c.id) as linked_project_names
      FROM contacts c`;
    if (where.length) q += ' WHERE ' + where.join(' AND ');
    q += ' ORDER BY c.priority ASC, c.name ASC';
    return db.prepare(q).all(...params);
  },

  getContact(id) {
    return db.prepare('SELECT * FROM contacts WHERE id = ?').get(id);
  },

  insertContact(c) {
    const rolesArr = c.roles
      ? (typeof c.roles === 'string' ? (() => { try { return JSON.parse(c.roles); } catch { return [c.roles]; } })() : c.roles)
      : (c.role ? [c.role] : []);
    return db.prepare(`
      INSERT INTO contacts (name, role, organization, email, phone, notes, priority, tags, photo_url, roles)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `).run(c.name, c.role || rolesArr[0] || null, c.organization || null, c.email || null, c.phone || null,
           c.notes || null, c.priority ?? 2, c.tags || '[]', c.photo_url || null, JSON.stringify(rolesArr));
  },

  updateContact(id, fields) {
    const allowed = ['name', 'role', 'organization', 'email', 'phone', 'notes', 'priority', 'tags', 'photo_url', 'education', 'extra_emails', 'website', 'industry_star', 'roles', 'extra_phones'];
    const f = { ...fields };

    // Synca role ↔ roles bidirektionellt
    if (f.roles !== undefined) {
      const arr = typeof f.roles === 'string'
        ? (() => { try { return JSON.parse(f.roles); } catch { return []; } })()
        : (Array.isArray(f.roles) ? f.roles : []);
      if (arr.length && f.role === undefined) f.role = arr[0];
    } else if (f.role !== undefined) {
      const cur = db.prepare('SELECT roles FROM contacts WHERE id = ?').get(id);
      const existing = (() => { try { return JSON.parse(cur?.roles || '[]'); } catch { return []; } })();
      if (f.role && !existing.includes(f.role)) {
        f.roles = JSON.stringify([f.role, ...existing].filter(Boolean));
      }
    }

    const sets = Object.keys(f).filter(k => allowed.includes(k) && f[k] !== undefined);
    if (!sets.length) return;
    const sql = `UPDATE contacts SET ${sets.map(k => k + ' = ?').join(', ')}, updated_at = datetime('now') WHERE id = ?`;
    db.prepare(sql).run(...sets.map(k => f[k]), id);
  },

  deleteContact(id) {
    db.prepare('DELETE FROM interactions WHERE contact_id = ?').run(id);
    db.prepare('DELETE FROM contact_projects WHERE contact_id = ?').run(id);
    db.prepare('DELETE FROM contact_organizations WHERE contact_id = ?').run(id);
    db.prepare('DELETE FROM contact_photos WHERE contact_id = ?').run(id);
    db.prepare('UPDATE emails SET matched_contact = NULL WHERE matched_contact = ?').run(id);
    db.prepare('UPDATE stipend_findings SET matched_contact = NULL WHERE matched_contact = ?').run(id);
    return db.prepare('DELETE FROM contacts WHERE id = ?').run(id);
  },

  // ── Interactions ───────────────────────────────────────────
  getInteractions(contactId) {
    return db.prepare('SELECT * FROM interactions WHERE contact_id = ? ORDER BY date DESC, created_at DESC').all(contactId);
  },

  insertInteraction(i) {
    return db.prepare(`
      INSERT INTO interactions (contact_id, date, type, summary, cv_sent, direction)
      VALUES (?, ?, ?, ?, ?, ?)
    `).run(i.contact_id, i.date, i.type || null, i.summary || null, i.cv_sent ? 1 : 0, i.direction || 'outgoing');
  },

  updateInteraction(id, fields) {
    const allowed = ['date', 'type', 'summary', 'cv_sent', 'direction'];
    const sets = Object.keys(fields).filter(k => allowed.includes(k) && fields[k] !== undefined);
    if (!sets.length) return;
    const sql = `UPDATE interactions SET ${sets.map(k => k + ' = ?').join(', ')} WHERE id = ?`;
    db.prepare(sql).run(...sets.map(k => fields[k]), id);
  },

  deleteInteraction(id) {
    return db.prepare('DELETE FROM interactions WHERE id = ?').run(id);
  },

  // ── Projects ───────────────────────────────────────────────
  getProjects({ status } = {}) {
    if (status) return db.prepare('SELECT * FROM projects WHERE status = ? ORDER BY start_date ASC').all(status);
    return db.prepare('SELECT * FROM projects ORDER BY start_date ASC').all();
  },

  getProject(id) {
    return db.prepare('SELECT * FROM projects WHERE id = ?').get(id);
  },

  insertProject(p) {
    return db.prepare(`
      INSERT INTO projects (title, type, organization, status, start_date, end_date, notes, own_work)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    `).run(p.title, p.type || null, p.organization || null, p.status || 'kommande',
           p.start_date || null, p.end_date || null, p.notes || null, p.own_work ? 1 : 0);
  },

  updateProject(id, fields) {
    const allowed = ['title', 'type', 'organization', 'status', 'start_date', 'end_date', 'notes', 'own_work', 'description', 'ai_data', 'jonna_role', 'jonna_notes', 'director', 'venue', 'num_performances', 'press_data', 'links', 'context_info'];
    const sets = Object.keys(fields).filter(k => allowed.includes(k) && fields[k] !== undefined);
    if (!sets.length) return;
    const sql = `UPDATE projects SET ${sets.map(k => k + ' = ?').join(', ')}, updated_at = datetime('now') WHERE id = ?`;
    db.prepare(sql).run(...sets.map(k => fields[k]), id);
  },

  deleteProject(id) {
    return db.prepare('DELETE FROM projects WHERE id = ?').run(id);
  },

  // ── contact_projects ───────────────────────────────────────
  getProjectContacts(projectId) {
    return db.prepare(`
      SELECT c.*, cp.role_in_project FROM contacts c
      JOIN contact_projects cp ON c.id = cp.contact_id
      WHERE cp.project_id = ?
    `).all(projectId);
  },

  getContactProjects(contactId) {
    return db.prepare(`
      SELECT p.*, cp.role_in_project FROM projects p
      JOIN contact_projects cp ON p.id = cp.project_id
      WHERE cp.contact_id = ?
      ORDER BY p.start_date ASC
    `).all(contactId);
  },

  linkContactProject(contactId, projectId, role) {
    try {
      db.prepare('INSERT INTO contact_projects (contact_id, project_id, role_in_project) VALUES (?, ?, ?)').run(contactId, projectId, role || null);
    } catch {}
  },

  unlinkContactProject(contactId, projectId) {
    db.prepare('DELETE FROM contact_projects WHERE contact_id = ? AND project_id = ?').run(contactId, projectId);
  },

  // ── Dashboard ──────────────────────────────────────────────
  getDashboard() {
    const upcomingDeadlines = db.prepare(`
      SELECT 'job' as type, title, organization, deadline, url FROM job_listings
        WHERE deadline IS NOT NULL AND deadline >= date('now')
      UNION ALL
      SELECT 'grant' as type, title, organization, deadline, url FROM grant_calls
        WHERE deadline IS NOT NULL AND deadline >= date('now')
      ORDER BY deadline ASC
      LIMIT 8
    `).all();

    const savedItems = db.prepare(`
      SELECT 'job' as type, title, organization, url, interesting_reason as reason, found_at FROM job_listings WHERE saved = 1
      UNION ALL
      SELECT 'grant' as type, title, organization, url, match_reason as reason, found_at FROM grant_calls WHERE saved = 1
      UNION ALL
      SELECT 'stipend' as type, person_name as title, organization, url, description as reason, found_at FROM stipend_findings WHERE saved = 1
      ORDER BY found_at DESC
      LIMIT 10
    `).all();

    const likedItems = db.prepare(`
      SELECT 'job' as type, title, organization, url, interesting_reason as reason FROM job_listings WHERE feedback = 1
      UNION ALL
      SELECT 'grant' as type, title, organization, url, match_reason as reason FROM grant_calls WHERE feedback = 1
      UNION ALL
      SELECT 'stipend' as type, person_name as title, organization, url, description as reason FROM stipend_findings WHERE feedback = 1
      ORDER BY title ASC
      LIMIT 8
    `).all();

    const latestFinds = db.prepare(`
      SELECT 'job' as type, title, organization, interesting_score as score, interesting_reason as reason, url, found_at
      FROM job_listings
      WHERE feedback != -1 AND saved = 0
      ORDER BY found_at DESC
      LIMIT 6
    `).all();

    const industryStarFollowup = db.prepare(`
      SELECT c.id, c.name, c.role, c.organization, c.photo_url, c.industry_star,
        MAX(i.date) as last_contact,
        CAST(julianday('now') - julianday(MAX(i.date)) AS INTEGER) as days_since
      FROM contacts c
      LEFT JOIN interactions i ON c.id = i.contact_id
      WHERE c.industry_star > 0
      GROUP BY c.id
      HAVING last_contact IS NULL OR days_since >= 30
      ORDER BY c.industry_star DESC, days_since DESC
      LIMIT 8
    `).all();

    const needsFollowup = db.prepare(`
      SELECT c.id, c.name, c.role, c.organization, c.photo_url,
        MAX(i.date) as last_contact,
        CAST(julianday('now') - julianday(MAX(i.date)) AS INTEGER) as days_since
      FROM contacts c
      LEFT JOIN interactions i ON c.id = i.contact_id
      WHERE (c.industry_star = 0 OR c.industry_star IS NULL) AND c.priority >= 5 AND c.priority < 10
      GROUP BY c.id
      HAVING last_contact IS NOT NULL AND days_since >= 30
      ORDER BY days_since DESC, c.priority ASC
      LIMIT 6
    `).all();

    const unparsedEmailContacts = db.prepare(`
      SELECT c.id, c.name, c.role, c.organization, c.photo_url,
        COUNT(e.id) as email_count
      FROM contacts c
      JOIN emails e ON e.matched_contact = c.id
      WHERE e.parsed = 0
      GROUP BY c.id
      ORDER BY email_count DESC, c.name ASC
      LIMIT 10
    `).all();

    const contactStipendNews = db.prepare(`
      SELECT s.id, s.person_name, s.organization, s.year, s.description, s.url, s.feedback, s.found_at,
        c.id as contact_id, c.name as contact_name, c.role as contact_role, c.photo_url
      FROM stipend_findings s
      JOIN contacts c ON s.matched_contact = c.id
      WHERE s.feedback != -1
      ORDER BY s.found_at DESC
      LIMIT 10
    `).all();

    return { upcomingDeadlines, savedItems, likedItems, latestFinds, industryStarFollowup, needsFollowup, contactStipendNews, unparsedEmailContacts };
  },

  getDashboardOpportunities() {
    const jobs    = db.prepare('SELECT id, title, organization, deadline, url FROM job_listings ORDER BY found_at DESC LIMIT 5').all();
    const stipends = db.prepare('SELECT id, person_name, organization, year, url FROM stipend_findings ORDER BY found_at DESC LIMIT 5').all();
    const grants  = db.prepare('SELECT id, title, organization, deadline, amount, url FROM grant_calls ORDER BY found_at DESC LIMIT 5').all();
    return { jobs, stipends, grants };
  },

  // ── Emails ─────────────────────────────────────────────────
  getContactEmails(contactId) {
    return db.prepare(`
      SELECT id, message_id, from_address, to_address, subject, body_text, received_at
      FROM emails
      WHERE matched_contact = ?
      ORDER BY received_at DESC
    `).all(contactId);
  },

  getOrgEmails(orgId) {
    return db.prepare(`
      SELECT id, message_id, from_address, to_address, subject, body_text, received_at
      FROM emails
      WHERE matched_organization = ?
      ORDER BY received_at DESC
      LIMIT 100
    `).all(orgId);
  },

  matchOrgEmails(orgId, genericEmails, domain) {
    // Match existing emails to this org by generic_emails and domain
    let count = 0;
    for (const addr of genericEmails) {
      const lower = addr.toLowerCase();
      const result = db.prepare(`
        UPDATE emails SET matched_organization = ?
        WHERE matched_organization IS NULL
        AND (LOWER(from_address) LIKE ? OR LOWER(to_address) LIKE ?)
      `).run(orgId, `%${lower}%`, `%${lower}%`);
      count += result.changes;
    }
    if (domain) {
      const lower = domain.toLowerCase();
      const result = db.prepare(`
        UPDATE emails SET matched_organization = ?
        WHERE matched_organization IS NULL
        AND (LOWER(from_address) LIKE ? OR LOWER(to_address) LIKE ?)
      `).run(orgId, `%@${lower}%`, `%@${lower}%`);
      count += result.changes;
    }
    return count;
  },

  // ── Jonna Profile ──────────────────────────────────────────
  getJonnaProfile() {
    const rows = db.prepare('SELECT key, value FROM jonna_profile').all();
    const result = {};
    for (const row of rows) {
      try { result[row.key] = JSON.parse(row.value); }
      catch { result[row.key] = row.value; }
    }
    return result;
  },

  getJonnaKey(key) {
    const row = db.prepare('SELECT value FROM jonna_profile WHERE key = ?').get(key);
    if (!row) return null;
    try { return JSON.parse(row.value); }
    catch { return row.value; }
  },

  setJonnaKey(key, value) {
    const serialized = typeof value === 'string' ? value : JSON.stringify(value);
    db.prepare(`
      INSERT INTO jonna_profile (key, value, updated_at) VALUES (?, ?, datetime('now'))
      ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_at = excluded.updated_at
    `).run(key, serialized);
  },

  getSchoolConnections(jonnaSchools) {
    // jonnaSchools: [{school: string, years: string|null}, ...]
    if (!jonnaSchools || !jonnaSchools.length) return [];

    function parseYears(str) {
      if (!str) return null;
      const nums = String(str).match(/\d{4}/g);
      if (!nums) return null;
      return { start: parseInt(nums[0]), end: parseInt(nums[nums.length - 1]) };
    }

    function overlap(y1, y2) {
      if (!y1 || !y2) return null; // unknown — can't determine
      return y1.start <= y2.end && y2.start <= y1.end;
    }

    const contacts = db.prepare('SELECT * FROM contacts').all();
    const results = [];

    for (const c of contacts) {
      const enrichment = (() => { try { return JSON.parse(c.enrichment_data || '{}'); } catch { return {}; } })();
      const contactColEdu = (() => { try { return JSON.parse(c.education || '[]'); } catch { return []; } })();
      const rawEdu = enrichment.education?.length ? enrichment.education : contactColEdu;
      const sharedEdu = enrichment.shared_education || [];
      // Normalize contact education to {school, years}, include manually linked schools
      const seen = new Set();
      const contactEdu = [...rawEdu, ...sharedEdu]
        .map(e => typeof e === 'string' ? { school: e, years: null } : e)
        .filter(e => { const k = (e.school || '').toLowerCase(); return k && !seen.has(k) && seen.add(k); });

      const matches = [];
      for (const je of jonnaSchools) {
        const jSchool = (je.school || '').toLowerCase();
        if (!jSchool) continue;
        for (const ce of contactEdu) {
          const cSchool = (ce.school || '').toLowerCase();
          if (!cSchool) continue;
          if (cSchool.includes(jSchool) || jSchool.includes(cSchool)) {
            const jYears = parseYears(je.years);
            const cYears = parseYears(ce.years);
            matches.push({
              school: je.school,
              jonna_years: je.years || null,
              contact_years: ce.years || null,
              overlaps: overlap(jYears, cYears),
            });
            break;
          }
        }
      }
      if (matches.length) results.push({ ...c, matched_schools: matches });
    }
    return results;
  },

  // ── Photos ─────────────────────────────────────────────────
  getContactPhotos(contactId) {
    return db.prepare('SELECT * FROM contact_photos WHERE contact_id = ? ORDER BY is_primary DESC, created_at ASC').all(contactId);
  },

  addContactPhoto(contactId, url, source = 'manual') {
    const result = db.prepare('INSERT INTO contact_photos (contact_id, url, source) VALUES (?, ?, ?)').run(contactId, url, source);
    const hasPrimary = db.prepare('SELECT id FROM contact_photos WHERE contact_id = ? AND is_primary = 1').get(contactId);
    if (!hasPrimary) {
      db.prepare('UPDATE contact_photos SET is_primary = 1 WHERE id = ?').run(result.lastInsertRowid);
      db.prepare('UPDATE contacts SET photo_url = ? WHERE id = ?').run(url, contactId);
    }
    return result;
  },

  deleteContactPhoto(id) {
    const photo = db.prepare('SELECT * FROM contact_photos WHERE id = ?').get(id);
    if (!photo) return;
    db.prepare('DELETE FROM contact_photos WHERE id = ?').run(id);
    if (photo.is_primary) {
      const next = db.prepare('SELECT * FROM contact_photos WHERE contact_id = ? ORDER BY created_at ASC LIMIT 1').get(photo.contact_id);
      if (next) {
        db.prepare('UPDATE contact_photos SET is_primary = 1 WHERE id = ?').run(next.id);
        db.prepare('UPDATE contacts SET photo_url = ? WHERE id = ?').run(next.url, photo.contact_id);
      } else {
        db.prepare('UPDATE contacts SET photo_url = NULL WHERE id = ?').run(photo.contact_id);
      }
    }
  },

  setPrimaryPhoto(contactId, photoId) {
    const photo = db.prepare('SELECT * FROM contact_photos WHERE id = ? AND contact_id = ?').get(photoId, contactId);
    if (!photo) return;
    db.prepare('UPDATE contact_photos SET is_primary = 0 WHERE contact_id = ?').run(contactId);
    db.prepare('UPDATE contact_photos SET is_primary = 1 WHERE id = ?').run(photoId);
    db.prepare('UPDATE contacts SET photo_url = ? WHERE id = ?').run(photo.url, contactId);
  },

  photoExists(contactId, url) {
    return !!db.prepare('SELECT id FROM contact_photos WHERE contact_id = ? AND url = ?').get(contactId, url);
  },

  clearEnrichment(contactId) {
    db.prepare(`UPDATE contacts SET enrichment_data = NULL, enriched_at = NULL WHERE id = ?`).run(contactId);
  },

  getLastInteractionDate(contactId) {
    return db.prepare(`SELECT MAX(date) as last_date FROM interactions WHERE contact_id = ?`).get(contactId);
  },

  getInteractionCount(contactId) {
    return db.prepare('SELECT COUNT(*) as count FROM interactions WHERE contact_id = ?').get(contactId);
  },

  getSentEmailSamples(ownEmail, limit = 20) {
    // Hämta mejl skickade av Jonna — med tillräckligt lång brödtext
    const addr = `%${ownEmail.trim().toLowerCase()}%`;
    return db.prepare(`
      SELECT subject, body_text FROM emails
      WHERE LOWER(from_address) LIKE ?
        AND body_text IS NOT NULL
        AND LENGTH(body_text) > 80
      ORDER BY received_at DESC
      LIMIT ?
    `).all(addr, limit);
  },

  saveEnrichment(contactId, data) {
    db.prepare(`UPDATE contacts SET enrichment_data = ?, enriched_at = datetime('now') WHERE id = ?`)
      .run(JSON.stringify(data), contactId);
  },

  // ── Job listings ───────────────────────────────────────────
  getJobListings({ saved } = {}) {
    if (saved !== undefined) return db.prepare('SELECT * FROM job_listings WHERE saved = ? ORDER BY found_at DESC').all(saved ? 1 : 0);
    return db.prepare('SELECT * FROM job_listings ORDER BY interesting_score DESC, found_at DESC').all();
  },

  insertJobListing(j) {
    return db.prepare(`
      INSERT INTO job_listings (title, organization, description, url, deadline, interesting_score, interesting_reason, known_contacts)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    `).run(j.title || null, j.organization || null, j.description || null, j.url || null,
           j.deadline || null, j.interesting_score || 0, j.interesting_reason || null, j.known_contacts || '[]');
  },

  saveJobListing(id, saved) {
    db.prepare('UPDATE job_listings SET saved = ? WHERE id = ?').run(saved ? 1 : 0, id);
  },

  clearJobListings() {
    db.prepare('DELETE FROM job_listings WHERE saved = 0').run();
  },

  // ── Stipend findings ───────────────────────────────────────
  getStipendFindings({ saved } = {}) {
    if (saved !== undefined) return db.prepare('SELECT s.*, c.name as contact_name FROM stipend_findings s LEFT JOIN contacts c ON s.matched_contact = c.id WHERE s.saved = ? ORDER BY s.found_at DESC').all(saved ? 1 : 0);
    return db.prepare('SELECT s.*, c.name as contact_name FROM stipend_findings s LEFT JOIN contacts c ON s.matched_contact = c.id ORDER BY s.found_at DESC').all();
  },

  insertStipendFinding(s) {
    return db.prepare(`
      INSERT INTO stipend_findings (person_name, organization, year, description, url, matched_contact)
      VALUES (?, ?, ?, ?, ?, ?)
    `).run(s.person_name || null, s.organization || null, s.year || null, s.description || null, s.url || null, s.matched_contact || null);
  },

  saveStipendFinding(id, saved) {
    db.prepare('UPDATE stipend_findings SET saved = ? WHERE id = ?').run(saved ? 1 : 0, id);
  },

  feedbackStipend(id, feedback) {
    db.prepare('UPDATE stipend_findings SET feedback = ? WHERE id = ?').run(feedback, id);
  },

  clearStipendFindings() {
    db.prepare('DELETE FROM stipend_findings WHERE saved = 0 AND feedback = 0').run();
  },

  feedbackJob(id, feedback) {
    db.prepare('UPDATE job_listings SET feedback = ? WHERE id = ?').run(feedback, id);
  },

  // ── Grant calls ────────────────────────────────────────────
  getGrantCalls() {
    return db.prepare('SELECT * FROM grant_calls ORDER BY feedback DESC, found_at DESC').all();
  },

  insertGrantCall(g) {
    return db.prepare(`
      INSERT INTO grant_calls (title, organization, description, url, deadline, amount, match_reason)
      VALUES (?, ?, ?, ?, ?, ?, ?)
    `).run(g.title || null, g.organization || null, g.description || null, g.url || null,
           g.deadline || null, g.amount || null, g.match_reason || null);
  },

  saveGrantCall(id, saved) {
    db.prepare('UPDATE grant_calls SET saved = ? WHERE id = ?').run(saved ? 1 : 0, id);
  },

  feedbackGrant(id, feedback) {
    db.prepare('UPDATE grant_calls SET feedback = ? WHERE id = ?').run(feedback, id);
  },

  clearGrantCalls() {
    db.prepare('DELETE FROM grant_calls WHERE saved = 0 AND feedback = 0').run();
  },

  getDiscoverFeedback() {
    const liked = [
      ...db.prepare('SELECT title, organization, interesting_reason as reason FROM job_listings WHERE feedback = 1').all().map(r => ({ type: 'jobb', ...r })),
      ...db.prepare('SELECT title, organization, match_reason as reason FROM grant_calls WHERE feedback = 1').all().map(r => ({ type: 'bidrag', ...r })),
      ...db.prepare('SELECT person_name as title, organization, description as reason FROM stipend_findings WHERE feedback = 1').all().map(r => ({ type: 'stipendiat', ...r })),
    ];
    const disliked = [
      ...db.prepare('SELECT title, organization FROM job_listings WHERE feedback = -1').all().map(r => ({ type: 'jobb', ...r })),
      ...db.prepare('SELECT title, organization FROM grant_calls WHERE feedback = -1').all().map(r => ({ type: 'bidrag', ...r })),
      ...db.prepare('SELECT person_name as title, organization FROM stipend_findings WHERE feedback = -1').all().map(r => ({ type: 'stipendiat', ...r })),
    ];
    return { liked, disliked };
  },

  // ── Emails (ingest) ────────────────────────────────────────
  insertEmail(e) {
    return db.prepare(`
      INSERT OR IGNORE INTO emails
      (message_id, from_address, to_address, subject, body_text, received_at, matched_contact, matched_organization)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    `).run(e.message_id || null, e.from_address || null, e.to_address || null,
           e.subject || null, e.body_text || null, e.received_at || null,
           e.matched_contact || null, e.matched_organization || null);
  },

  getAllEmails({ limit = 100 } = {}) {
    return db.prepare(`
      SELECT e.*, c.name as contact_name
      FROM emails e
      LEFT JOIN contacts c ON e.matched_contact = c.id
      ORDER BY e.received_at DESC LIMIT ?
    `).all(limit);
  },

  matchEmail(emailId, contactId) {
    db.prepare('UPDATE emails SET matched_contact = ? WHERE id = ?').run(contactId, emailId);
  },

  getUnmatchedSenderCounts() {
    return db.prepare(`
      SELECT from_address, COUNT(*) as count
      FROM emails
      WHERE matched_contact IS NULL AND from_address IS NOT NULL
      GROUP BY from_address
      ORDER BY count DESC
    `).all();
  },

  getSubjectsForSender(addr) {
    return db.prepare(`
      SELECT subject FROM emails
      WHERE from_address LIKE ? AND subject IS NOT NULL
      LIMIT 5
    `).all(`%${addr}%`).map(r => r.subject);
  },

  getContactEmailsFull(contactId) {
    return db.prepare(`
      SELECT subject, body_text, from_address, to_address, received_at
      FROM emails WHERE matched_contact = ? ORDER BY received_at ASC LIMIT 50
    `).all(contactId);
  },

  findEmailsByAddress(email, contactId) {
    return db.prepare(`
      SELECT * FROM emails
      WHERE (LOWER(from_address) LIKE ? OR LOWER(to_address) LIKE ?)
      AND (matched_contact IS NULL OR matched_contact = ?)
      ORDER BY received_at ASC
    `).all(`%${email}%`, `%${email}%`, contactId);
  },

  getInteractionBySubject(contactId, summary) {
    return db.prepare('SELECT id FROM interactions WHERE contact_id = ? AND summary = ?')
      .get(contactId, summary);
  },

  // ── Organizations ──────────────────────────────────────────
  getOrganizations() {
    const orgs = db.prepare('SELECT * FROM organizations ORDER BY name ASC').all();
    return orgs.map(o => ({
      ...o,
      generic_emails: (() => { try { return JSON.parse(o.generic_emails || '[]'); } catch { return []; } })(),
      contacts: db.prepare(`
        SELECT c.id, c.name, c.role, c.email, c.photo_url FROM contacts c
        JOIN contact_organizations co ON c.id = co.contact_id
        WHERE co.organization_id = ?
        ORDER BY c.name ASC
      `).all(o.id),
    }));
  },

  getOrganization(id) {
    const o = db.prepare('SELECT * FROM organizations WHERE id = ?').get(id);
    if (!o) return null;
    return {
      ...o,
      generic_emails: (() => { try { return JSON.parse(o.generic_emails || '[]'); } catch { return []; } })(),
      contacts: db.prepare(`
        SELECT c.id, c.name, c.role, c.email, c.photo_url FROM contacts c
        JOIN contact_organizations co ON c.id = co.contact_id
        WHERE co.organization_id = ?
        ORDER BY c.name ASC
      `).all(id),
    };
  },

  insertOrganization(o) {
    return db.prepare(`
      INSERT INTO organizations (name, domain, email, type, notes, generic_emails)
      VALUES (?, ?, ?, ?, ?, ?)
    `).run(o.name, o.domain || null, o.email || null, o.type || null, o.notes || null,
           JSON.stringify(o.generic_emails || []));
  },

  updateOrganization(id, fields) {
    const allowed = ['name', 'domain', 'email', 'type', 'notes', 'generic_emails'];
    const sets = Object.keys(fields).filter(k => allowed.includes(k) && fields[k] !== undefined);
    if (!sets.length) return;
    const values = sets.map(k => k === 'generic_emails' ? JSON.stringify(fields[k]) : fields[k]);
    const sql = `UPDATE organizations SET ${sets.map(k => k + ' = ?').join(', ')}, updated_at = datetime('now') WHERE id = ?`;
    db.prepare(sql).run(...values, id);
  },

  deleteOrganization(id) {
    return db.prepare('DELETE FROM organizations WHERE id = ?').run(id);
  },

  setOrgSummary(id, summary) {
    db.prepare(`UPDATE organizations SET ai_summary = ?, updated_at = datetime('now') WHERE id = ?`).run(summary, id);
  },

  linkContactOrganization(contactId, organizationId) {
    try {
      db.prepare('INSERT INTO contact_organizations (contact_id, organization_id) VALUES (?, ?)').run(contactId, organizationId);
    } catch {}
  },

  unlinkContactOrganization(contactId, organizationId) {
    db.prepare('DELETE FROM contact_organizations WHERE contact_id = ? AND organization_id = ?').run(contactId, organizationId);
  },

  getContactOrganizations(contactId) {
    return db.prepare(`
      SELECT o.* FROM organizations o
      JOIN contact_organizations co ON o.id = co.organization_id
      WHERE co.contact_id = ?
      ORDER BY o.name ASC
    `).all(contactId);
  },

  getOrgColleagues(contactId) {
    // Other contacts who share at least one organization with this contact
    return db.prepare(`
      SELECT DISTINCT c.id, c.name, c.role, c.roles, c.photo_url,
        o.name as org_name
      FROM contacts c
      JOIN contact_organizations co ON c.id = co.contact_id
      JOIN contact_organizations co2 ON co.organization_id = co2.organization_id
      JOIN organizations o ON o.id = co.organization_id
      WHERE co2.contact_id = ? AND c.id != ?
      ORDER BY c.name ASC
    `).all(contactId, contactId);
  },

  getOrganizationByDomain(domain) {
    return db.prepare('SELECT * FROM organizations WHERE domain = ?').get(domain);
  },

  // ── Admin ──────────────────────────────────────────────────
  clearContactsAndEmails() {
    db.prepare('DELETE FROM interactions').run();
    db.prepare('DELETE FROM contact_projects').run();
    db.prepare('DELETE FROM contact_photos').run();
    db.prepare('DELETE FROM contact_organizations').run();
    db.prepare('DELETE FROM contacts').run();
    db.prepare('DELETE FROM emails').run();
    db.prepare('DELETE FROM organizations').run();
    const keepKeys = ['bio', 'cv', 'writing_style', 'self_search_results', 'manual_skills', 'education', 'discover_sites_jobs', 'discover_sites_stipends', 'discover_sites_grants'];
    db.prepare(`DELETE FROM jonna_profile WHERE key NOT IN (${keepKeys.map(() => '?').join(',')})`).run(...keepKeys);
  },

  // ── Org-sync ───────────────────────────────────────────────
  findOrCreateOrganization(name) {
    if (!name?.trim()) return null;
    const clean = name.trim();
    const existing = db.prepare('SELECT id FROM organizations WHERE LOWER(name) = LOWER(?)').get(clean);
    if (existing) return existing.id;
    const result = db.prepare('INSERT INTO organizations (name) VALUES (?)').run(clean);
    return result.lastInsertRowid;
  },

  syncContactOrganization(contactId, orgName) {
    if (!orgName?.trim()) return;
    const orgId = this.findOrCreateOrganization(orgName.trim());
    if (!orgId) return;
    try {
      db.prepare('INSERT OR IGNORE INTO contact_organizations (contact_id, organization_id) VALUES (?, ?)').run(contactId, orgId);
    } catch {}
  },

  syncAllContactOrganizations() {
    const contacts = db.prepare("SELECT id, organization FROM contacts WHERE organization IS NOT NULL AND organization != ''").all();
    let synced = 0;
    for (const c of contacts) {
      this.syncContactOrganization(c.id, c.organization);
      synced++;
    }
    return synced;
  },

  // ── Merge helpers ──────────────────────────────────────────
  moveInteractions(fromId, toId) {
    db.prepare('UPDATE interactions SET contact_id = ? WHERE contact_id = ?').run(toId, fromId);
  },
  movePhotos(fromId, toId) {
    db.prepare('UPDATE contact_photos SET contact_id = ? WHERE contact_id = ?').run(toId, fromId);
  },
  moveEmails(fromId, toId) {
    db.prepare('UPDATE emails SET matched_contact = ? WHERE matched_contact = ?').run(toId, fromId);
  },
  moveStipends(fromId, toId) {
    db.prepare('UPDATE stipend_findings SET matched_contact = ? WHERE matched_contact = ?').run(toId, fromId);
  },

  // ── Discover raw queries ────────────────────────────────────
  getStipendNames() {
    return db.prepare('SELECT person_name FROM stipend_findings').all().map(r => r.person_name).filter(Boolean);
  },
  getStipendKeys() {
    return db.prepare('SELECT person_name, organization, year FROM stipend_findings').all();
  },
  getJobTitles() {
    return db.prepare('SELECT title, organization FROM job_listings').all().map(r => `${r.title} (${r.organization})`).filter(Boolean);
  },
  getGrantTitles() {
    return db.prepare('SELECT title, organization FROM grant_calls').all().map(r => `${r.title} (${r.organization})`).filter(Boolean);
  },
  getContactStipends(contactId) {
    return db.prepare('SELECT * FROM stipend_findings WHERE matched_contact = ? ORDER BY year DESC, found_at DESC').all(contactId);
  },
  linkStipendToContact(stipendId, contactId) {
    db.prepare('UPDATE stipend_findings SET matched_contact = ? WHERE id = ?').run(contactId, stipendId);
  },
  getStipendById(id) {
    return db.prepare('SELECT * FROM stipend_findings WHERE id = ?').get(id);
  },
  updateStipendContact(stipendId, contactId) {
    db.prepare('UPDATE stipend_findings SET matched_contact = ? WHERE id = ?').run(contactId, stipendId);
  },
  getJobById(id) {
    return db.prepare('SELECT * FROM job_listings WHERE id = ?').get(id);
  },
  updateJobKnownContacts(jobId, json) {
    db.prepare('UPDATE job_listings SET known_contacts = ? WHERE id = ?').run(json, jobId);
  },

  // ── Applications ───────────────────────────────────────────
  getApplications({ status, limit = 50 } = {}) {
    if (status) return db.prepare('SELECT * FROM applications WHERE status = ? ORDER BY created_at DESC LIMIT ?').all(status, limit);
    return db.prepare('SELECT * FROM applications ORDER BY created_at DESC LIMIT ?').all(limit);
  },

  getApplication(id) {
    return db.prepare('SELECT * FROM applications WHERE id = ?').get(id);
  },

  insertApplication(a) {
    return db.prepare(`
      INSERT INTO applications (opportunity_type, opportunity_id, opportunity_title, opportunity_organization, opportunity_deadline, opportunity_url, document_type, status, generated_text, edited_text, ai_analysis, generation_prompt, notes)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `).run(
      a.opportunity_type || 'manual', a.opportunity_id || null, a.opportunity_title || null,
      a.opportunity_organization || null, a.opportunity_deadline || null, a.opportunity_url || null,
      a.document_type || 'personal_letter', a.status || 'draft',
      a.generated_text || null, a.edited_text || null, a.ai_analysis || null,
      a.generation_prompt || null, a.notes || null
    );
  },

  updateApplication(id, fields) {
    const allowed = ['document_type', 'status', 'generated_text', 'edited_text', 'ai_analysis', 'generation_prompt', 'notes', 'sent_at', 'opportunity_title', 'opportunity_organization', 'opportunity_deadline', 'opportunity_url'];
    const sets = Object.keys(fields).filter(k => allowed.includes(k) && fields[k] !== undefined);
    if (!sets.length) return;
    const sql = `UPDATE applications SET ${sets.map(k => k + ' = ?').join(', ')}, updated_at = datetime('now') WHERE id = ?`;
    db.prepare(sql).run(...sets.map(k => fields[k]), id);
  },

  deleteApplication(id) {
    return db.prepare('DELETE FROM applications WHERE id = ?').run(id);
  },

  getCastings({ limit = 50 } = {}) {
    return db.prepare(`SELECT * FROM job_listings WHERE opportunity_subtype IN ('casting', 'audition', 'open_call') ORDER BY found_at DESC LIMIT ?`).all(limit);
  },

  setJobSubtype(id, subtype) {
    db.prepare('UPDATE job_listings SET opportunity_subtype = ? WHERE id = ?').run(subtype, id);
  },

  clearCastings() {
    db.prepare(`DELETE FROM job_listings WHERE opportunity_subtype IN ('casting', 'audition', 'open_call') AND saved = 0`).run();
  },

  // ── AI helper ──────────────────────────────────────────────
  getContactWithInteractions(id) {
    const contact = db.prepare('SELECT * FROM contacts WHERE id = ?').get(id);
    if (!contact) return null;
    const interactions = db.prepare('SELECT * FROM interactions WHERE contact_id = ? ORDER BY date DESC LIMIT 5').all(id);
    return { contact, interactions };
  },

  // ── Network graph ──────────────────────────────────────────
  getNetworkData() {
    const contacts = db.prepare('SELECT * FROM contacts').all();
    const jonnaRow = db.prepare("SELECT value FROM jonna_profile WHERE key = 'full_name'").get();
    const jonnaName = jonnaRow ? jonnaRow.value : 'Jonna';

    const contactProjects = db.prepare('SELECT contact_id, project_id FROM contact_projects').all();
    const contactOrgs = db.prepare('SELECT contact_id, organization_id FROM contact_organizations').all();

    // Count connections per contact (will be updated as edges are built)
    const connectionCount = {};
    for (const c of contacts) connectionCount[c.id] = 0;

    const nodes = [
      { id: 'jonna', label: jonnaName, type: 'jonna' },
      ...contacts.map(c => ({
        id: c.id,
        label: c.name,
        role: c.role || null,
        photo_url: c.photo_url || null,
        connections: 0,
      })),
    ];

    const edges = [];
    const contactContactSeen = new Set();

    function addContactContactEdge(aId, bId, type, label) {
      const key = `${Math.min(aId, bId)}-${Math.max(aId, bId)}-${type}`;
      if (contactContactSeen.has(key)) return;
      contactContactSeen.add(key);
      edges.push({ source: aId, target: bId, type, label: label || null });
      connectionCount[aId] = (connectionCount[aId] || 0) + 1;
      connectionCount[bId] = (connectionCount[bId] || 0) + 1;
    }

    // Normalize school name → stable key for deduplication
    function schoolKey(name) {
      return (name || '').toLowerCase().replace(/\s+/g, ' ').replace(/[().,–\-]/g, '').trim();
    }

    // Build school nodes + edges via enrichment_data
    const schoolNodes = {}; // key → node object
    const schoolEdgesSeen = new Set();
    const jonnaContactSeen = new Set();

    for (const c of contacts) {
      let enrichment = {};
      try { enrichment = JSON.parse(c.enrichment_data || '{}'); } catch {}

      // shared_productions → direct jonna↔contact edge
      const prods = enrichment.shared_productions;
      if (Array.isArray(prods) && prods.length > 0) {
        const label = prods[0].title || prods[0] || null;
        const key = `jonna-${c.id}-shared_production`;
        if (!jonnaContactSeen.has(key)) {
          jonnaContactSeen.add(key);
          edges.push({ source: 'jonna', target: c.id, type: 'shared_production', label: typeof label === 'string' ? label : null });
          connectionCount[c.id] = (connectionCount[c.id] || 0) + 1;
        }
      }

      // shared_education → route through a school node
      const edus = enrichment.shared_education;
      if (Array.isArray(edus) && edus.length > 0) {
        for (const edu of edus) {
          const rawName = (typeof edu === 'string' ? edu : edu?.school) || '';
          if (!rawName) continue;
          const sKey = schoolKey(rawName);
          const nodeId = 'school:' + sKey;

          // Create school node once (use first seen name as canonical label)
          if (!schoolNodes[sKey]) {
            // Shorten very long school names for display
            let displayName = rawName.replace(/\(.*?\)/g, '').trim(); // strip parenthetical
            if (displayName.length > 35) displayName = displayName.slice(0, 33) + '…';
            schoolNodes[sKey] = { id: nodeId, label: displayName, type: 'school' };
          }

          // Jonna → school (once per school)
          const jonnaSchoolKey = 'jonna:' + sKey;
          if (!schoolEdgesSeen.has(jonnaSchoolKey)) {
            schoolEdgesSeen.add(jonnaSchoolKey);
            edges.push({ source: 'jonna', target: nodeId, type: 'shared_education', label: null });
          }

          // contact → school (once per contact per school)
          const contactSchoolKey = `${c.id}:${sKey}`;
          if (!schoolEdgesSeen.has(contactSchoolKey)) {
            schoolEdgesSeen.add(contactSchoolKey);
            edges.push({ source: c.id, target: nodeId, type: 'shared_education', label: null });
            connectionCount[c.id] = (connectionCount[c.id] || 0) + 1;
          }
        }
      }
    }

    // Add school nodes to the nodes array
    nodes.push(...Object.values(schoolNodes));

    // contact → contact edges via shared CRM project
    const projectGroups = {};
    for (const row of contactProjects) {
      if (!projectGroups[row.project_id]) projectGroups[row.project_id] = [];
      projectGroups[row.project_id].push(row.contact_id);
    }
    for (const group of Object.values(projectGroups)) {
      for (let i = 0; i < group.length; i++) {
        for (let j = i + 1; j < group.length; j++) {
          addContactContactEdge(group[i], group[j], 'shared_project', null);
        }
      }
    }

    // contact → org node edges (one node per organization, named)
    const orgsById = {};
    for (const row of db.prepare('SELECT id, name FROM organizations').all()) {
      orgsById[row.id] = row.name;
    }
    const orgNodesSeen = new Set();
    const orgEdgesSeen = new Set();
    const orgGroups = {};
    for (const row of contactOrgs) {
      if (!orgGroups[row.organization_id]) orgGroups[row.organization_id] = [];
      orgGroups[row.organization_id].push(row.contact_id);
    }
    for (const [orgId, members] of Object.entries(orgGroups)) {
      if (members.length < 2) continue; // skip orgs with only one contact
      const orgName = orgsById[orgId] || `Organisation ${orgId}`;
      const nodeId = 'org:' + orgId;

      // Create org node once
      if (!orgNodesSeen.has(nodeId)) {
        orgNodesSeen.add(nodeId);
        let displayName = orgName.length > 30 ? orgName.slice(0, 28) + '…' : orgName;
        nodes.push({ id: nodeId, label: displayName, type: 'organization' });
      }

      // Each member → org node
      for (const contactId of members) {
        const edgeKey = `${contactId}:${nodeId}`;
        if (!orgEdgesSeen.has(edgeKey)) {
          orgEdgesSeen.add(edgeKey);
          edges.push({ source: contactId, target: nodeId, type: 'shared_org', label: null });
          connectionCount[contactId] = (connectionCount[contactId] || 0) + 1;
        }
      }
    }

    // contact → contact edges via matched_colleagues
    for (const c of contacts) {
      let enrichment = {};
      try { enrichment = JSON.parse(c.enrichment_data || '{}'); } catch {}
      const colleagues = enrichment.matched_colleagues;
      if (Array.isArray(colleagues)) {
        for (const col of colleagues) {
          if (col.id && col.id !== c.id) {
            addContactContactEdge(c.id, col.id, 'colleague', null);
          }
        }
      }
    }

    // Update connection counts on nodes
    for (const node of nodes) {
      if (node.id !== 'jonna') {
        node.connections = connectionCount[node.id] || 0;
      }
    }

    return { nodes, edges };
  },
};
