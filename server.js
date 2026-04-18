require('dotenv').config();
const express = require('express');
const basicAuth = require('express-basic-auth');
const path = require('path');
const fs = require('fs');
const os = require('os');
const readline = require('readline');
const Anthropic = require('@anthropic-ai/sdk');
const { ImapFlow } = require('imapflow');
const { simpleParser } = require('mailparser');
const cheerio = require('cheerio');
const db = require('./db');

// Ensure uploads dir exists
const uploadsDir = process.env.UPLOADS_PATH || path.join(__dirname, 'public', 'uploads');
fs.mkdirSync(uploadsDir, { recursive: true });

const app = express();
app.use(basicAuth({
  users: { [process.env.AUTH_USER || 'jonna']: process.env.AUTH_PASSWORD || 'changeme' },
  challenge: true
}));
app.use(express.json({ limit: '50mb' }));
app.use(express.text({ limit: '500mb', type: 'text/plain' }));
app.use(express.static(path.join(__dirname, 'public')));

const anthropic = new Anthropic();

// ── Organizations ──────────────────────────────────────────────
app.get('/api/organizations', (req, res) => {
  res.json(db.getOrganizations());
});

app.post('/api/organizations', (req, res) => {
  const { name, domain, type, notes, generic_emails } = req.body;
  if (!name) return res.status(400).json({ error: 'name krävs' });
  const result = db.insertOrganization({ name, domain, type, notes, generic_emails });
  res.json({ id: result.lastInsertRowid });
});

app.get('/api/organizations/:id', (req, res) => {
  const org = db.getOrganization(req.params.id);
  if (!org) return res.status(404).json({ error: 'Saknas' });
  res.json(org);
});

app.patch('/api/organizations/:id', (req, res) => {
  db.updateOrganization(req.params.id, req.body);
  res.json(db.getOrganization(req.params.id));
});

app.delete('/api/organizations/:id', (req, res) => {
  db.deleteOrganization(req.params.id);
  res.json({ ok: true });
});

app.post('/api/organizations/:id/enrich', async (req, res) => {
  const org = db.getOrganization(req.params.id);
  if (!org) return res.status(404).json({ error: 'Saknas' });

  const contactNames = org.contacts.slice(0, 10)
    .map(c => c.name + (c.role ? ` (${c.role})` : '')).join(', ');
  const jonnaContext = getJonnaContext();

  const prompt = `Sök information om "${org.name}"${org.domain ? ` (webbplats: ${org.domain})` : ''} — en organisation inom svensk teater, film eller scenkonst.
${contactNames ? `Kopplade kontakter i systemet: ${contactNames}` : ''}
${jonnaContext ? `Kontext om användaren (skådespelerska): ${jonnaContext.slice(0, 300)}` : ''}

Svara ENBART med JSON:
{
  "type": "ett av exakt dessa: Statlig teater | Fri teater | Filmbolag | Produktionsbolag | Kulturinstitution | Agent/Management | Utbildning | Övrigt",
  "summary": "2-3 meningar om organisationen — vad de gör, deras profil och relevans för en skådespelerska"
}`;

  try {
    const text = await claudeSearch(prompt, 800, 'claude-haiku-4-5-20251001', false);
    let result;
    try {
      const m = text.match(/\{[\s\S]*\}/);
      result = JSON.parse(m ? m[0] : text);
    } catch {
      result = { type: null, summary: '' };
    }
    if (result.summary) db.setOrgSummary(req.params.id, result.summary);
    res.json(result);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.post('/api/organizations/:id/contacts/:contactId', (req, res) => {
  db.linkContactOrganization(req.params.contactId, req.params.id);
  res.json({ ok: true });
});

app.delete('/api/organizations/:id/contacts/:contactId', (req, res) => {
  db.unlinkContactOrganization(req.params.contactId, req.params.id);
  res.json({ ok: true });
});

app.get('/api/organizations/:id/emails', (req, res) => {
  res.json(db.getOrgEmails(req.params.id));
});

app.post('/api/organizations/:id/match-emails', (req, res) => {
  const org = db.getOrganization(req.params.id);
  if (!org) return res.status(404).json({ error: 'Saknas' });
  const matched = db.matchOrgEmails(org.id, org.generic_emails || [], org.domain || '');
  res.json({ matched });
});

app.get('/api/contacts/:id/organizations', (req, res) => {
  res.json(db.getContactOrganizations(req.params.id));
});

app.get('/api/contacts/:id/org-colleagues', (req, res) => {
  res.json(db.getOrgColleagues(req.params.id));
});

// ── Contacts ──────────────────────────────────────────────────
app.get('/api/contacts', (req, res) => {
  const { search, role, priority } = req.query;
  res.json(db.getContacts({ search, role, priority }));
});

app.post('/api/contacts', (req, res) => {
  const { name } = req.body;
  if (!name) return res.status(400).json({ error: 'Namn krävs' });
  const result = db.insertContact(req.body);
  const id = result.lastInsertRowid;
  if (req.body.organization) db.syncContactOrganization(id, req.body.organization);
  res.json({ id, ...req.body });
});

app.post('/api/contacts/sync-organizations', (req, res) => {
  const synced = db.syncAllContactOrganizations();
  res.json({ synced });
});

app.get('/api/contacts/:id', (req, res) => {
  const contact = db.getContact(req.params.id);
  if (!contact) return res.status(404).json({ error: 'Kontakt saknas' });
  res.json(contact);
});

app.patch('/api/contacts/:id', (req, res) => {
  db.updateContact(req.params.id, req.body);
  if (req.body.organization) db.syncContactOrganization(req.params.id, req.body.organization);
  res.json({ ok: true });
});

app.delete('/api/contacts/:id', (req, res) => {
  try {
    db.deleteContact(req.params.id);
    res.json({ ok: true });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Retroaktivt matcha befintliga mejl till en kontakt + skapa interaktioner
app.post('/api/contacts/:id/match-emails', (req, res) => {
  const contact = db.getContact(req.params.id);
  if (!contact) return res.json({ matched: 0 });

  const extras = (() => { try { return JSON.parse(contact.extra_emails || '[]'); } catch { return []; } })();
  const allEmails = [contact.email, ...extras].filter(Boolean).map(e => e.toLowerCase());
  if (!allEmails.length) return res.json({ matched: 0 });

  let matched = 0;
  for (const email of allEmails) {
    const emails = db.findEmailsByAddress(email, contact.id);
    for (const e of emails) {
      db.matchEmail(e.id, contact.id);
      const existing = db.getInteractionBySubject(contact.id, e.subject || '');
      if (!existing && e.subject) {
        const direction = e.from_address?.toLowerCase().includes(email) ? 'incoming' : 'outgoing';
        db.insertInteraction({
          contact_id: contact.id,
          date: e.received_at ? e.received_at.slice(0, 10) : new Date().toISOString().slice(0, 10),
          type: 'E-post',
          summary: e.subject,
          direction,
        });
      }
      matched++;
    }
  }
  res.json({ matched });
});

// ── Interactions ───────────────────────────────────────────────
app.get('/api/contacts/:id/interactions', (req, res) => {
  res.json(db.getInteractions(req.params.id));
});

app.post('/api/contacts/:id/interactions', (req, res) => {
  const { date } = req.body;
  if (!date) return res.status(400).json({ error: 'Datum krävs' });
  const result = db.insertInteraction({ ...req.body, contact_id: req.params.id });
  res.json({ id: result.lastInsertRowid });
});

app.patch('/api/interactions/:id', (req, res) => {
  db.updateInteraction(req.params.id, req.body);
  res.json({ ok: true });
});

app.delete('/api/interactions/:id', (req, res) => {
  db.deleteInteraction(req.params.id);
  res.json({ ok: true });
});

// ── Projects ───────────────────────────────────────────────────
app.get('/api/projects', (req, res) => {
  const { status } = req.query;
  res.json(db.getProjects({ status }));
});

app.post('/api/projects', (req, res) => {
  const { title } = req.body;
  if (!title) return res.status(400).json({ error: 'Titel krävs' });
  const result = db.insertProject(req.body);
  res.json({ id: result.lastInsertRowid, ...req.body });
});

app.get('/api/projects/:id', (req, res) => {
  const project = db.getProject(req.params.id);
  if (!project) return res.status(404).json({ error: 'Projekt saknas' });
  res.json(project);
});

app.patch('/api/projects/:id', (req, res) => {
  db.updateProject(req.params.id, req.body);
  res.json({ ok: true });
});

app.delete('/api/projects/:id', (req, res) => {
  db.deleteProject(req.params.id);
  res.json({ ok: true });
});

app.post('/api/projects/import-from-profile', (req, res) => {
  const cvParsed = db.getJonnaKey('cv_parsed') || {};
  const selfSearch = db.getJonnaKey('self_search_results') || {};
  const manual = db.getJonnaKey('manual_productions') || [];

  // Samla alla produktioner från alla källor
  const all = [
    ...(cvParsed.productions || []),
    ...(selfSearch.productions || []),
    ...manual,
  ];

  // Deduplicera på titel
  const seen = new Set();
  const unique = all.filter(p => {
    const k = (p.title || '').toLowerCase().trim();
    if (!k || seen.has(k)) return false;
    seen.add(k);
    return true;
  });

  // Hämta befintliga projekt för att inte skapa dubletter
  const existing = db.getProjects({});
  const existingTitles = new Set(existing.map(p => p.title.toLowerCase().trim()));

  let created = 0;
  for (const p of unique) {
    const title = (p.title || '').trim();
    if (!title || existingTitles.has(title.toLowerCase())) continue;
    db.insertProject({
      title,
      organization: p.theater || null,
      type: 'Teater',
      status: 'avslutad',
      start_date: p.year ? `${p.year}-01-01` : null,
      notes: p.role ? `Jonnas roll: ${p.role}` : null,
      own_work: 0,
    });
    created++;
  }

  res.json({ created, total: unique.length });
});

// ── Project AI enrich ─────────────────────────────────────────
app.post('/api/projects/:id/enrich', async (req, res) => {
  const project = db.getProject(req.params.id);
  if (!project) return res.status(404).json({ error: 'Projekt saknas' });
  try {
    const links = (() => { try { return JSON.parse(project.links || '[]'); } catch { return []; } })();
    const linkHint = links.length ? `\nSök SPECIFIKT på dessa sidor:\n${links.map(l => l.url).join('\n')}` : '';
    const contextHint = project.context_info ? `\nExtra info om produktionen:\n${project.context_info}` : '';

    const prompt = `Sök information om produktionen "${project.title}"${project.organization ? ' av ' + project.organization : ''} inom svensk teater, film eller scenkonstvärlden.${linkHint}${contextHint}

Svara ENBART med JSON:
{
  "organization": "teater eller produktionsbolag",
  "type": "Teater|Film|TV|Kortfilm|Webserier|Reklam|Dubbing|Annat",
  "start_year": "2023",
  "end_year": "2024",
  "director": "regissörens namn",
  "venue": "scen/plats t.ex. Stora scenen, Kulturhuset",
  "num_performances": "t.ex. 24 föreställningar eller speltid",
  "description": "beskrivning av produktionen (2-4 meningar)",
  "cast": [{"name": "Förnamn Efternamn", "role": "titel/roll i produktionen"}],
  "sources": ["url1"]
}

Försök hitta regissör, scen och så många medverkande som möjligt. Om inget hittas, returnera tomma strängar/arrayer. Svara ENBART med JSON.`;
    const text = await claudeSearch(prompt, 1500, 'claude-haiku-4-5-20251001');
    let info = {};
    try {
      const m = text.match(/\{[\s\S]*\}/);
      info = JSON.parse(m ? m[0] : '{}');
    } catch { info = {}; }

    // Läs befintlig ai_data för att kunna slå ihop
    const existingAiData = (() => { try { return JSON.parse(project.ai_data || '{}'); } catch { return {}; } })();
    const existingCast = existingAiData.cast || [];

    // Separera Jonnas info från övrig cast
    const jonnaName = (db.getJonnaKey('full_name') || '').toLowerCase();
    const jonnaFirstName = jonnaName.split(' ')[0];
    const allContacts = db.getContacts();

    let jonnaInCast = null;
    const newCast = [];
    for (const person of (info.cast || [])) {
      const lower = person.name.toLowerCase();
      const isJonna = jonnaName && (lower.includes(jonnaName) || jonnaName.includes(lower) ||
        (jonnaFirstName && lower.startsWith(jonnaFirstName)));
      if (isJonna) {
        jonnaInCast = person;
      } else {
        const match = allContacts.find(c => c.name.toLowerCase() === lower ||
          c.name.toLowerCase().includes(lower) || lower.includes(c.name.toLowerCase()));
        newCast.push({ ...person, existing_contact: match ? { id: match.id, name: match.name } : null });
      }
    }

    // Slå ihop cast – lägg till nya personer, behåll befintliga
    const mergedCastMap = new Map(existingCast.map(p => [p.name.toLowerCase(), p]));
    for (const p of newCast) {
      if (!mergedCastMap.has(p.name.toLowerCase())) mergedCastMap.set(p.name.toLowerCase(), p);
    }
    const mergedCast = [...mergedCastMap.values()];

    // Slå ihop övrig data – fyll bara i tomma fält, slå ihop arrayer
    const mergedSources = [...new Set([...(existingAiData.sources || []), ...(info.sources || [])])];
    const mergedInfo = { ...existingAiData, ...Object.fromEntries(
      Object.entries(info).filter(([k, v]) => v && !existingAiData[k] && k !== 'sources' && k !== 'cast')
    ), cast: mergedCast, sources: mergedSources };

    // Spara Jonnas roll + nya fält om hittade (skriv inte över befintliga)
    const projectUpdate = { ai_data: JSON.stringify(mergedInfo) };
    if (jonnaInCast?.role && !project.jonna_role) projectUpdate.jonna_role = jonnaInCast.role;
    if (info.director && !project.director) projectUpdate.director = info.director;
    if (info.venue && !project.venue) projectUpdate.venue = info.venue;
    if (info.num_performances && !project.num_performances) projectUpdate.num_performances = info.num_performances;

    db.updateProject(req.params.id, projectUpdate);
    res.json({ ...mergedInfo, jonna_in_cast: jonnaInCast });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ── Project press search ───────────────────────────────────────
app.post('/api/projects/:id/press', async (req, res) => {
  const project = db.getProject(req.params.id);
  if (!project) return res.status(404).json({ error: 'Projekt saknas' });
  try {
    const links = (() => { try { return JSON.parse(project.links || '[]'); } catch { return []; } })();
    const linkHint = links.length ? `\nSök SPECIFIKT även på dessa sidor:\n${links.map(l => l.url).join('\n')}` : '';
    const contextHint = project.context_info ? `\nExtra info: ${project.context_info}` : '';

    const prompt = `Sök efter pressrecensioner, tidningsartiklar och kritik om föreställningen/produktionen "${project.title}"${project.organization ? ' av ' + project.organization : ''}${project.start_date ? ' (' + project.start_date + ')' : ''} i Sverige.${linkHint}${contextHint}

Sök i Dagens Nyheter, Svenska Dagbladet, Göteborgs-Posten, Dagens Teater, Scen & Film, lokaltidningar och andra svenska medier.

Svara ENBART med JSON:
{
  "reviews": [
    {
      "publication": "tidningens namn",
      "author": "recensentens namn om känt",
      "date": "publiceringsdatum",
      "rating": "betyg om angivet t.ex. 4/5",
      "quote": "det viktigaste citatet från recensionen (1-2 meningar)",
      "url": "länk om känd",
      "sentiment": "positiv|neutral|negativ"
    }
  ],
  "mentions": [
    {
      "publication": "källa",
      "date": "datum",
      "context": "kort beskrivning av vad som nämndes",
      "url": "länk om känd"
    }
  ]
}

Om inget hittas, returnera tomma arrayer. Svara ENBART med JSON.`;
    const text = await claudeSearch(prompt, 2000, 'claude-haiku-4-5-20251001', false);
    let pressData = {};
    try {
      const m = text.match(/\{[\s\S]*\}/);
      pressData = JSON.parse(m ? m[0] : '{}');
    } catch { pressData = { reviews: [], mentions: [] }; }

    db.updateProject(req.params.id, { press_data: JSON.stringify(pressData) });
    res.json(pressData);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ── Jonna deep search ──────────────────────────────────────────
app.post('/api/jonna/deep-search', async (req, res) => {
  const jonnaName = db.getJonnaKey('full_name') || 'Jonna Ljunggren';
  const cvParsed = db.getJonnaKey('cv_parsed') || {};
  const productions = (cvParsed.productions || []).slice(0, 5).map(p => p.title).join(', ');
  try {
    const prompt = `Sök brett efter information om den svenska scenkonstnären ${jonnaName}.

Sök efter:
- Intervjuer och reportage
- Artiklar där ${jonnaName} nämns
- Recensioner där hon omnämns specifikt
- Sociala medier och professionella profiler
- Branschsidor (Teateralliansen, STIM, IMDb etc.)
${productions ? `\nKända produktioner att söka på: ${productions}` : ''}

Svara ENBART med JSON:
{
  "interviews": [{"source": "...", "date": "...", "summary": "...", "url": "..."}],
  "mentions": [{"source": "...", "date": "...", "context": "...", "url": "..."}],
  "profiles": [{"platform": "...", "url": "...", "description": "..."}],
  "skills_found": ["färdighet1", "färdighet2"],
  "facts": ["intressant faktum 1", "intressant faktum 2"]
}

Svara ENBART med JSON.`;
    const text = await claudeSearch(prompt, 2500, 'claude-haiku-4-5-20251001', false);
    let data = {};
    try {
      const m = text.match(/\{[\s\S]*\}/);
      data = JSON.parse(m ? m[0] : '{}');
    } catch { data = {}; }

    // Spara i self_search_results och slå ihop med befintliga
    const existing = db.getJonnaKey('self_search_results') || {};
    const merged = {
      ...existing,
      interviews: [...(existing.interviews || []), ...(data.interviews || [])],
      mentions: [...(existing.mentions || []), ...(data.mentions || [])],
      profiles: [...(existing.profiles || []), ...(data.profiles || [])],
      skills_found: [...new Set([...(existing.skills_found || []), ...(data.skills_found || [])])],
      facts: [...new Set([...(existing.facts || []), ...(data.facts || [])])],
      last_deep_search: new Date().toISOString(),
    };
    db.setJonnaKey('self_search_results', merged);
    res.json(data);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ── Jonna info bank ───────────────────────────────────────────
app.get('/api/jonna/infobank', (req, res) => {
  const profile = db.getJonnaProfile();
  const projects = db.getProjects({});

  // ── Grundprofil
  const name = profile.full_name || 'Jonna';
  const bio = profile.bio || '';
  const manualSkills = profile.manual_skills || [];
  const selfSearch = profile.self_search_results || {};
  const cvParsed = profile.cv_parsed || {};
  const artisticStatement = profile.artistic_statement || '';
  const writingStyle = profile.writing_style || null;
  let appSamples = [];
  try { appSamples = typeof profile.application_samples === 'string' ? JSON.parse(profile.application_samples) : (profile.application_samples || []); } catch {}

  // ── Utbildning — alla källor
  const manualEdu = profile.manual_education || [];
  const cvEdu = cvParsed.education || [];
  const aiEdu = selfSearch.education || [];
  const allEdu = [...manualEdu, ...cvEdu, ...aiEdu].reduce((acc, e) => {
    const key = (e.school || '').toLowerCase();
    if (key && !acc.find(x => (x.school || '').toLowerCase() === key)) acc.push(e);
    return acc;
  }, []);

  // ── Kompetenser
  const skillsFound = selfSearch.skills_found || [];
  const allSkills = [...new Set([...manualSkills, ...skillsFound])];

  // ── Produktioner — bygg detaljerad lista från projects-tabellen
  const productions = projects.map(p => {
    const press = (() => { try { return JSON.parse(p.press_data || '{}'); } catch { return {}; } })();
    const pressQuotes = (press.reviews || [])
      .filter(r => r.quote)
      .map(r => `${r.publication ? r.publication + ': ' : ''}"${r.quote}"${r.rating ? ' (' + r.rating + ')' : ''}`);

    return {
      title: p.title,
      organization: p.organization || null,
      type: p.type || null,
      years: [p.start_date, p.end_date].filter(Boolean).join('–') || null,
      jonna_role: p.jonna_role || null,
      director: p.director || null,
      venue: p.venue || null,
      num_performances: p.num_performances || null,
      description: p.description || null,
      press_quotes: pressQuotes,
      own_work: !!p.own_work,
    };
  }).filter(p => p.title);

  // ── Intervjuer & omnämnanden från djupdykning
  const interviews = selfSearch.interviews || [];
  const mentions = selfSearch.mentions || [];
  const facts = selfSearch.facts || [];
  const profiles = selfSearch.profiles || [];

  // ── Generera löptext för AI-kontext
  const lines = [];
  lines.push(`# ${name} — Informationsbank`);
  lines.push(`Genererad: ${new Date().toLocaleDateString('sv-SE')}\n`);

  if (bio) { lines.push(`## Bio\n${bio}\n`); }

  if (allEdu.length) {
    lines.push('## Utbildning');
    allEdu.forEach(e => lines.push(`- ${e.school}${e.years ? ' (' + e.years + ')' : ''}`));
    lines.push('');
  }

  if (allSkills.length) {
    lines.push(`## Kompetenser\n${allSkills.join(', ')}\n`);
  }

  if (productions.length) {
    lines.push('## Produktioner');
    productions.forEach(p => {
      lines.push(`\n### ${p.title}${p.years ? ' (' + p.years + ')' : ''}`);
      if (p.organization) lines.push(`Producent/teater: ${p.organization}`);
      if (p.type) lines.push(`Typ: ${p.type}`);
      if (p.jonna_role) lines.push(`Jonnas roll: ${p.jonna_role}`);
      if (p.director) lines.push(`Regi: ${p.director}`);
      if (p.venue) lines.push(`Scen: ${p.venue}`);
      if (p.num_performances) lines.push(`Speltid: ${p.num_performances}`);
      if (p.own_work) lines.push(`(Eget projekt)`);
      if (p.description) lines.push(`\n${p.description}`);
      if (p.press_quotes.length) {
        lines.push('\nPress:');
        p.press_quotes.forEach(q => lines.push(`  ${q}`));
      }
    });
    lines.push('');
  }

  if (interviews.length) {
    lines.push('## Intervjuer & reportage');
    interviews.forEach(i => lines.push(`- ${i.source}${i.date ? ' (' + i.date + ')' : ''}${i.summary ? ': ' + i.summary : ''}`));
    lines.push('');
  }

  if (mentions.length) {
    lines.push('## Omnämnanden i media');
    mentions.forEach(m => lines.push(`- ${m.source}${m.date ? ' (' + m.date + ')' : ''}${m.context ? ': ' + m.context : ''}`));
    lines.push('');
  }

  if (facts.length) {
    lines.push('## Övriga fakta');
    facts.forEach(f => lines.push(`- ${f}`));
    lines.push('');
  }

  if (profiles.length) {
    lines.push('## Profiler & länkar');
    profiles.forEach(p => lines.push(`- ${p.platform || ''}${p.url ? ': ' + p.url : ''}`));
    lines.push('');
  }

  if (artisticStatement) {
    lines.push(`## Konstnärligt statement\n${artisticStatement}\n`);
  }

  if (writingStyle) {
    lines.push('## Skrivstil');
    if (writingStyle.tone) lines.push(`Ton: ${writingStyle.tone}`);
    if (writingStyle.style_notes) lines.push(writingStyle.style_notes);
    if (writingStyle.example_phrases?.length) lines.push(`Typiska fraser: ${writingStyle.example_phrases.join(' / ')}`);
    if (writingStyle.analysis) lines.push(`\n${writingStyle.analysis}`);
    lines.push('');
  }

  if (appSamples.length) {
    lines.push('## Exempelansökningar');
    appSamples.forEach((s, i) => {
      lines.push(`\n### Exempel ${i + 1}${s.title ? ': ' + s.title : ''}`);
      if (s.text) lines.push(s.text.slice(0, 800));
    });
    lines.push('');
  }

  const text = lines.join('\n');

  res.json({
    text,
    structured: { name, bio, education: allEdu, skills: allSkills, productions, interviews, mentions, facts, profiles },
    stats: {
      productions: productions.length,
      with_description: productions.filter(p => p.description).length,
      with_press: productions.filter(p => p.press_quotes.length).length,
      with_role: productions.filter(p => p.jonna_role).length,
      education: allEdu.length,
      skills: allSkills.length,
      interviews: interviews.length,
      mentions: mentions.length,
    }
  });
});

// ── Contact–Project links ──────────────────────────────────────
app.get('/api/projects/:id/contacts', (req, res) => {
  res.json(db.getProjectContacts(req.params.id));
});

app.get('/api/contacts/:id/projects', (req, res) => {
  res.json(db.getContactProjects(req.params.id));
});

app.post('/api/projects/:id/contacts/:contactId', (req, res) => {
  db.linkContactProject(req.params.contactId, req.params.id, req.body.role_in_project);
  res.json({ ok: true });
});

app.delete('/api/projects/:id/contacts/:contactId', (req, res) => {
  db.unlinkContactProject(req.params.contactId, req.params.id);
  res.json({ ok: true });
});

// Ta bort en person ur ai_data.cast (efter manuell koppling)
app.post('/api/projects/:id/remove-cast-person', (req, res) => {
  const { name } = req.body;
  if (!name) return res.status(400).json({ error: 'name krävs' });
  const project = db.getProject(req.params.id);
  if (!project) return res.status(404).json({ error: 'Projekt saknas' });
  const aiData = (() => { try { return JSON.parse(project.ai_data || '{}'); } catch { return {}; } })();
  const nameLower = name.toLowerCase();
  aiData.cast = (aiData.cast || []).filter(p => (p.name || '').toLowerCase() !== nameLower);
  db.updateProject(req.params.id, { ai_data: JSON.stringify(aiData) });
  res.json({ ok: true });
});

// ── Dashboard ──────────────────────────────────────────────────
app.post('/api/contacts/auto-priority', (req, res) => {
  const contacts = db.getContacts({});
  const now = Date.now();
  let updated = 0;

  for (const c of contacts) {
    // Tidpoäng (0–9) baserat på dagar sedan senaste kontakt
    const lastInteraction = db.getLastInteractionDate(c.id);
    const lastDate = lastInteraction?.last_date ? new Date(lastInteraction.last_date).getTime() : null;
    const daysSince = lastDate ? Math.floor((now - lastDate) / 86400000) : null;

    let tidPoäng;
    if (daysSince === null)       tidPoäng = 2;
    else if (daysSince <= 14)     tidPoäng = 9;
    else if (daysSince <= 30)     tidPoäng = 8;
    else if (daysSince <= 90)     tidPoäng = 7;
    else if (daysSince <= 180)    tidPoäng = 6;
    else if (daysSince <= 365)    tidPoäng = 5;
    else if (daysSince <= 540)    tidPoäng = 4;
    else if (daysSince <= 720)    tidPoäng = 3;
    else                          tidPoäng = 2;

    // Interaktionsbonus
    const { count: interactionCount } = db.getInteractionCount(c.id);
    let interaktionsBonus;
    if (interactionCount >= 10)      interaktionsBonus = 1.5;
    else if (interactionCount >= 5)  interaktionsBonus = 1;
    else if (interactionCount >= 2)  interaktionsBonus = 0.5;
    else                             interaktionsBonus = 0;

    // Rollbonus
    const contactRoles = (() => { try { return JSON.parse(c.roles || '[]'); } catch { return []; } })();
    if (c.role && !contactRoles.length) contactRoles.push(c.role);
    const priorityRoles = ['regissör', 'producent', 'casting', 'agent', 'koreograf'];
    const rollBonus = contactRoles.some(r => priorityRoles.some(p => r.toLowerCase().includes(p))) ? 1 : 0;

    // Produktionsbonus
    const enrichment = (() => { try { return JSON.parse(c.enrichment_data || '{}'); } catch { return {}; } })();
    const produktionsBonus = (enrichment.shared_productions?.length > 0) ? 1 : 0;

    const priority = Math.min(10, Math.round(tidPoäng + interaktionsBonus + rollBonus + produktionsBonus));

    db.updateContact(c.id, { priority });
    updated++;
  }

  res.json({ updated });
});

// ── AI-rankning av branschvikt ─────────────────────────────────
app.post('/api/contacts/ai-rank-batch', async (req, res) => {
  const BATCH = 25;
  const offset = parseInt(req.body?.offset ?? 0);

  // Hämta alla kontakter sorterade på priority (varmast först)
  const all = db.getContacts({}).sort((a, b) => (b.priority || 5) - (a.priority || 5));
  const batch = all.slice(offset, offset + BATCH);
  if (!batch.length) return res.json({ results: [], total: all.length, offset, done: true });

  const jonnaContext = getJonnaContext();

  const contactLines = batch.map((c, i) => {
    const enrichment = (() => { try { return JSON.parse(c.enrichment_data || '{}'); } catch { return {}; } })();
    const bio = (enrichment.bio || '').slice(0, 150);
    const prods = (enrichment.shared_productions || []).map(p => p.title).slice(0, 3).join(', ');
    const interactionCount = db.getInteractionCount(c.id)?.count || 0;
    return `${i + 1}. ${c.name} | Roll: ${c.role || '?'} | Org: ${c.organization || c.org_names || '?'} | Interaktioner: ${interactionCount}${bio ? ' | Bio: ' + bio : ''}${prods ? ' | Gem. produktioner: ' + prods : ''}`;
  }).join('\n');

  const prompt = `Du bedömer hur branschviktiga dessa kontakter är för en skådespelerska inom svensk teater/film och hur relevanta de är för framtida jobbmöjligheter.

${jonnaContext ? `OM SKÅDESPELERSKAN:\n${jonnaContext}\n` : ''}

Ge varje kontakt ett poäng 1–5:
5 = Nyckelperson i branschen, hög sannolikhet att ge jobb (regissörer, castingchefer, agenter på stora scener)
4 = Viktig branschperson, god potential
3 = Relevant kontakt, möjlig framtida samarbetspartner
2 = Perifer branschkoppling eller oklar relevans
1 = Troligen inte relevant för karriären

KONTAKTER:
${contactLines}

Svara ENBART med JSON-array (en post per kontakt, i samma ordning):
[{"index":1,"score":4,"reason":"Regissör på Dramaten, gemensam produktion"}]`;

  try {
    const msg = await anthropic.messages.create({
      model: 'claude-haiku-4-5-20251001',
      max_tokens: 1500,
      messages: [{ role: 'user', content: prompt }],
    });
    const m = msg.content[0].text.match(/\[[\s\S]*\]/);
    const rankings = JSON.parse(m ? m[0] : '[]');

    const results = [];
    for (const r of rankings) {
      const contact = batch[r.index - 1];
      if (!contact || !r.score) continue;
      db.updateContact(contact.id, { industry_star: Math.min(5, Math.max(0, Math.round(r.score))) });
      // Spara motivering i enrichment_data
      const enrichment = (() => { try { return JSON.parse(contact.enrichment_data || '{}'); } catch { return {}; } })();
      db.saveEnrichment(contact.id, { ...enrichment, ai_rank_reason: r.reason || null });
      results.push({ id: contact.id, name: contact.name, score: r.score, reason: r.reason });
    }

    res.json({ results, total: all.length, offset, nextOffset: offset + BATCH, done: offset + BATCH >= all.length });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.get('/api/network', (req, res) => {
  res.json(db.getNetworkData());
});

app.get('/api/dashboard', (req, res) => {
  res.json(db.getDashboard());
});

app.get('/api/discover/status', (req, res) => {
  res.json({
    last_searched_jobs:     db.getJonnaKey('last_searched_jobs')     || null,
    last_searched_stipends: db.getJonnaKey('last_searched_stipends') || null,
    last_searched_grants:   db.getJonnaKey('last_searched_grants')   || null,
    last_searched_castings: db.getJonnaKey('last_searched_castings') || null,
  });
});

app.get('/api/dashboard/opportunities', (req, res) => {
  const jobs = db.prepare('SELECT id, title, organization, deadline, url FROM job_listings ORDER BY found_at DESC LIMIT 5').all();
  const stipends = db.prepare('SELECT id, person_name, organization, year, url FROM stipend_findings ORDER BY found_at DESC LIMIT 5').all();
  const grants = db.prepare('SELECT id, title, organization, deadline, amount, url FROM grant_calls ORDER BY found_at DESC LIMIT 5').all();
  res.json({ jobs, stipends, grants });
});

// ── AI: suggest followup ───────────────────────────────────────
app.post('/api/contacts/:id/suggest-followup', async (req, res) => {
  const data = db.getContactWithInteractions(req.params.id);
  if (!data) return res.status(404).json({ error: 'Kontakt saknas' });
  const { contact, interactions } = data;

  const interactionText = interactions.length
    ? interactions.map(i => `- ${i.date} [${i.type || 'övrigt'}${i.cv_sent ? ', CV skickat' : ''}]: ${i.summary || '(ingen sammanfattning)'}`).join('\n')
    : '(inga tidigare interaktioner)';

  const jonnaContext = getJonnaContext();
  const styleBlock = jonnaContext ? `\nOm Jonna:\n${jonnaContext}\n` : '';

  try {
    const msg = await anthropic.messages.create({
      model: 'claude-haiku-4-5-20251001',
      max_tokens: 400,
      messages: [{
        role: 'user',
        content: `Du hjälper skådespelaren Jonna att hålla kontakten med viktiga personer i teater- och filmbranschen.
${styleBlock}
Kontakt:
- Namn: ${contact.name}
- Roll: ${contact.role || 'okänd'}
- Organisation: ${contact.organization || 'okänd'}

Senaste interaktioner:
${interactionText}

Skriv ett kort, personligt och professionellt förslag på uppföljningsmeddelande som Jonna kan skicka till ${contact.name}. Meddelandet ska vara på svenska, vara naturligt och inte kännas maskinskrivet. Max 5 meningar. Svara bara med meddelandetexten.`
      }]
    });
    res.json({ suggestion: msg.content[0].text.trim() });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ── AI: filtrera e-postavsändare i omgångar ────────────────────
// Slår ihop avsändarlista med AI-analys och filtrerar bort irrelevanta
function applyAnalysis(senders, analysis) {
  return senders.map((s, i) => {
    const a = analysis.find(x => x.index === i + 1) || {};
    return { ...s, relevant: a.relevant !== false, unsure: !!a.unsure, reason: a.reason || '', suggested_role: a.suggested_role || '' };
  }).filter(s => s.relevant);
}

async function filterEmailSendersBatched(senders, batchSize = 50) {
  const analysis = [];
  const total = Math.ceil(senders.length / batchSize);
  for (let i = 0; i < senders.length; i += batchSize) {
    const batch = senders.slice(i, i + batchSize);
    const batchNum = Math.floor(i / batchSize) + 1;
    console.log(`[AI-filter] Omgång ${batchNum}/${total} (${i + 1}–${Math.min(i + batchSize, senders.length)} av ${senders.length})...`);
    const batchAnalysis = await filterEmailSenders(batch);
    console.log(`[AI-filter] Omgång ${batchNum}/${total} klar — ${batchAnalysis.filter(a => a.relevant !== false).length} relevanta`);
    batchAnalysis.forEach(a => analysis.push({ ...a, index: i + a.index }));
  }
  return analysis;
}

// ── AI: filtrera e-postavsändare ───────────────────────────────
async function filterEmailSenders(senders) {
  if (!senders.length) return [];
  const senderList = senders.map((s, i) =>
    `${i + 1}. Namn: "${s.name}" | E-post: ${s.address} | Antal mejl: ${s.count}${s.subjects?.length ? ' | Ämnen: ' + s.subjects.slice(0, 3).join(' / ') : ''}`
  ).join('\n');

  // Hämta inlärda exempel från Jonnas feedback
  const feedback = db.getJonnaKey('email_sender_feedback') || [];
  const approved = feedback.filter(f => f.decision === 'approved').slice(-10);
  const rejected = feedback.filter(f => f.decision === 'rejected').slice(-10);
  let feedbackSection = '';
  if (approved.length || rejected.length) {
    feedbackSection = '\nJONNA HAR LÄRT MIG:\n';
    if (approved.length) feedbackSection += 'Godkände (lägga till i CRM):\n' + approved.map(f => `- "${f.name}" <${f.address}>${f.reason ? ' — ' + f.reason : ''}`).join('\n') + '\n';
    if (rejected.length) feedbackSection += 'Avvisade (inte relevant):\n' + rejected.map(f => `- "${f.name}" <${f.address}>${f.reason ? ' — ' + f.reason : ''}`).join('\n') + '\n';
    feedbackSection += 'VIKTIGT: Avvisningar gäller ENBART den specifika e-postadressen — inte hela domänen. En avvisad noreply@teater.se betyder INTE att eva@teater.se ska avvisas.\n';
    feedbackSection += 'Använd dessa exempel för att kalibrera din bedömning av liknande avsändare.\n';
  }

  const jonnaContext = getJonnaContext();

  const apiCall = anthropic.messages.create({
    model: 'claude-haiku-4-5-20251001',
    max_tokens: 1500,
    messages: [{ role: 'user', content: `Du filtrerar e-postkontakter för en skådespelerskas professionella CRM inom teater och film.
${jonnaContext ? `\nOM JONNA (använd detta för att bedöma om en avsändare är relevant för henne):\n${jonnaContext}\n` : ''}

FILTRERA BORT (sätt relevant: false):
- Butiker, matvaror, e-handel, restauranger
- Nyhetsbrev, marknadsföring, reklam
- Banker, försäkringsbolag, myndigheter, Skatteverket, Försäkringskassan
- Automatiska bekräftelser, kvitton, orderbekräftelser
- Noreply-adresser eller adresser med "no-reply", "info@", "newsletter@", "support@", "hello@" från företag
- Streamingtjänster, appar, sociala medier (Spotify, Netflix, etc.)
- Allt som uppenbart INTE är en riktig människa som skriver personligt

BEHÅLL (sätt relevant: true):
- Riktiga människor som skrivit personliga mejl, oavsett domän (gmail, hotmail, etc.)
- Folk från teater, film, kultur, media, utbildning
- Agenter, producenter, regissörer, skådespelare, koreografer, musiker
- Kollegor, samarbetspartners, bekanta inom branschen
- Journalister, kritiker, festivalarrangörer
${feedbackSection}
Om du är osäker — sätt relevant: true och unsure: true så att Jonna kan granska manuellt.

Avsändare:
${senderList}

Svara ENBART med JSON-array: [{"index":1,"relevant":true,"unsure":false,"reason":"Personlig konversation, trolig branschkontakt","suggested_role":"regissör"}]` }]
  });
  const timeout = new Promise((_, reject) =>
    setTimeout(() => reject(new Error('Timeout efter 45s')), 45000)
  );
  const msg = await Promise.race([apiCall, timeout]);

  try {
    const m = msg.content[0].text.match(/\[[\s\S]*\]/);
    return JSON.parse(m ? m[0] : '[]');
  } catch { return []; }
}

// ── Jonna context (gemensamt för alla AI-anrop) ────────────────
function getJonnaContext() {
  const profile = db.getJonnaProfile();
  const parts = [];

  const bio = profile.bio || profile.self_search_results?.bio || '';
  if (bio) parts.push(`Bio: ${bio}`);

  const cv = profile.cv_parsed || {};
  if (cv.education?.length) parts.push(`Utbildning: ${cv.education.map(e => typeof e === 'string' ? e : `${e.school}${e.years ? ' ('+e.years+')' : ''}`).join(', ')}`);
  if (cv.productions?.length) parts.push(`Produktioner (CV): ${cv.productions.slice(0, 10).map(p => `${p.title}${p.theater ? ' vid '+p.theater : ''}${p.year ? ' ('+p.year+')' : ''}`).join(', ')}`);
  if (cv.organizations?.length) parts.push(`Organisationer: ${cv.organizations.join(', ')}`);

  const aiSkills = profile.self_search_results?.skills || [];
  const manualSkills = profile.manual_skills || [];
  const allSkills = [...new Set([...manualSkills, ...aiSkills])];
  if (allSkills.length) parts.push(`Kompetenser: ${allSkills.join(', ')}`);

  const style = profile.writing_style;
  if (style?.tone) parts.push(`Skrivstil: ${style.tone}. ${style.analysis || ''}`);
  if (style?.example_phrases?.length) parts.push(`Typiska fraser: ${style.example_phrases.join(', ')}`);

  const selfSearch = profile.self_search_results;
  if (selfSearch?.productions?.length) {
    const prods = selfSearch.productions.slice(0, 8).map(p => `${p.title}${p.theater ? ' vid '+p.theater : ''}`).join(', ');
    parts.push(`Ytterligare produktioner (webbsökning): ${prods}`);
  }

  try {
    const feedback = db.getDiscoverFeedback();
    if (feedback.liked.length) parts.push(`Intressant för Jonna: ${feedback.liked.slice(0, 10).map(l => l.title).join(', ')}`);
    if (feedback.disliked.length) parts.push(`Inte intressant för Jonna: ${feedback.disliked.slice(0, 5).map(l => l.title).join(', ')}`);
  } catch {}

  const full = parts.join('\n');
  return full.slice(0, 1200); // cap för att hålla nere tokens
}

// ── Kort Jonna-summering för jobbannonssökning ─────────────────
function getJonnaSearchSummary() {
  const profile = db.getJonnaProfile();
  const parts = ['Jonna är skådespelerska/performer inom svensk teater och film.'];

  const aiSkills = profile.self_search_results?.skills || [];
  const manualSkills = profile.manual_skills || [];
  const allSkills = [...new Set([...manualSkills, ...aiSkills])].slice(0, 8);
  if (allSkills.length) parts.push(`Kompetenser: ${allSkills.join(', ')}.`);

  try {
    const feedback = db.getDiscoverFeedback();
    if (feedback.liked.length) parts.push(`Har visat intresse för: ${feedback.liked.slice(0, 5).map(l => l.title).join(', ')}.`);
  } catch {}

  return parts.join(' ');
}

// ── Jonna full context (för ansökningsskrivning — ingen cap) ──────
function getJonnaFullContext() {
  const profile = db.getJonnaProfile();
  const parts = [];

  const bio = profile.bio || profile.self_search_results?.bio || '';
  if (bio) parts.push(`BIO:\n${bio}`);

  const cv = profile.cv_parsed || {};
  if (cv.name) parts.push(`Namn: ${cv.name}`);
  if (cv.education?.length) parts.push(`Utbildning: ${cv.education.map(e => typeof e === 'string' ? e : `${e.school}${e.years ? ' (' + e.years + ')' : ''}`).join(', ')}`);

  let actorAttrs = null;
  try { actorAttrs = typeof profile.actor_attributes === 'string' ? JSON.parse(profile.actor_attributes) : profile.actor_attributes; } catch {}
  if (actorAttrs) {
    const attrParts = [];
    if (actorAttrs.height_cm) attrParts.push(`längd: ${actorAttrs.height_cm} cm`);
    if (actorAttrs.playing_age || (actorAttrs.age_range_min && actorAttrs.age_range_max)) attrParts.push(`spelålder: ${actorAttrs.playing_age || actorAttrs.age_range_min + '–' + actorAttrs.age_range_max}`);
    if (actorAttrs.voice_type) attrParts.push(`röst: ${actorAttrs.voice_type}`);
    if (actorAttrs.singing && actorAttrs.singing_range) attrParts.push(`sång: ${actorAttrs.singing_range}`);
    if (actorAttrs.languages?.length) attrParts.push(`språk: ${actorAttrs.languages.join(', ')}`);
    if (actorAttrs.dialects?.length) attrParts.push(`dialekter: ${actorAttrs.dialects.join(', ')}`);
    if (actorAttrs.movement_skills?.length) attrParts.push(`rörelse: ${actorAttrs.movement_skills.join(', ')}`);
    if (actorAttrs.union) attrParts.push(`fackförbund: ${actorAttrs.union}`);
    if (attrParts.length) parts.push(`Skådespelarattribut: ${attrParts.join(', ')}`);
  }

  const aiSkills = profile.self_search_results?.skills || [];
  const manualSkills = profile.manual_skills || [];
  const allSkills = [...new Set([...manualSkills, ...aiSkills])];
  if (allSkills.length) parts.push(`Kompetenser: ${allSkills.join(', ')}`);

  const artistic = profile.artistic_statement;
  if (artistic) parts.push(`KONSTNÄRLIGT STATEMENT:\n${artistic}`);

  // Produktioner från CRM
  try {
    const projects = db.getProjects().filter(p => p.jonna_role);
    if (projects.length) {
      const prodLines = projects.slice(0, 10).map(p => {
        let line = `- ${p.title}`;
        if (p.start_date) line += ` (${p.start_date.slice(0, 4)})`;
        if (p.jonna_role) line += ` som ${p.jonna_role}`;
        if (p.director) line += `, regi ${p.director}`;
        if (p.venue || p.organization) line += ` — ${p.venue || p.organization}`;
        let press = '';
        try { const pd = typeof p.press_data === 'string' ? JSON.parse(p.press_data) : p.press_data; if (pd?.reviews?.length) press = pd.reviews[0].slice(0, 120); } catch {}
        if (press) line += `\n  Press: "${press}"`;
        return line;
      });
      parts.push(`PRODUKTIONER (CRM):\n${prodLines.join('\n')}`);
    }
  } catch {}

  if (cv.productions?.length) {
    const cvProds = cv.productions.slice(0, 8).map(p => `- ${p.title}${p.theater ? ' vid ' + p.theater : ''}${p.year ? ' (' + p.year + ')' : ''}${p.role ? ' som ' + p.role : ''}`);
    parts.push(`PRODUKTIONER (CV):\n${cvProds.join('\n')}`);
  }

  const selfSearch = profile.self_search_results;
  if (selfSearch?.productions?.length) {
    const webProds = selfSearch.productions.slice(0, 6).map(p => `- ${p.title}${p.theater ? ' vid ' + p.theater : ''}`);
    parts.push(`PRODUKTIONER (webbsökning):\n${webProds.join('\n')}`);
  }

  const style = profile.writing_style;
  if (style?.tone) parts.push(`SKRIVSTIL:\nTon: ${style.tone}.\n${style.analysis || ''}`);
  if (style?.example_phrases?.length) parts.push(`Typiska fraser: ${style.example_phrases.join(' / ')}`);

  // Ansökningsexempel (max 2)
  let appSamples = null;
  try { appSamples = typeof profile.application_samples === 'string' ? JSON.parse(profile.application_samples) : profile.application_samples; } catch {}
  if (Array.isArray(appSamples) && appSamples.length) {
    const excerpts = appSamples.slice(0, 2).map(s => `Exempelansökan — ${s.title}:\n${(s.text || '').slice(0, 500)}`);
    parts.push(`TIDIGARE ANSÖKNINGAR (stil och ton):\n${excerpts.join('\n\n')}`);
  }

  return parts.join('\n\n');
}

// ── Helper: Claude with web search (server-side tool) ──────────
async function claudeSearch(prompt, maxTokens = 2000, model = 'claude-sonnet-4-6', useWebSearch = true) {
  const messages = [{ role: 'user', content: prompt }];
  const tools = useWebSearch ? [{ type: 'web_search_20250305', name: 'web_search' }] : undefined;
  let retried = false;
  for (let i = 0; i < 3; i++) {
    let resp;
    try {
      resp = await anthropic.messages.create({ model, max_tokens: maxTokens, ...(tools && { tools }), messages });
    } catch (err) {
      if (!retried && (err.status === 429 || err.status === 529)) {
        retried = true;
        const wait = err.status === 529 ? 30000 : 60000;
        console.log(`[claudeSearch] ${err.status === 529 ? 'Overloaded' : 'Rate limit'} — väntar ${wait / 1000}s...`);
        await new Promise(r => setTimeout(r, wait));
        resp = await anthropic.messages.create({ model, max_tokens: maxTokens, ...(tools && { tools }), messages });
      } else throw err;
    }
    messages.push({ role: 'assistant', content: resp.content });
    if (resp.stop_reason === 'end_turn') {
      return resp.content.filter(b => b.type === 'text').map(b => b.text).join('').trim();
    }
    if (resp.stop_reason === 'tool_use') {
      const toolResults = resp.content
        .filter(b => b.type === 'tool_use')
        .map(b => ({ type: 'tool_result', tool_use_id: b.id, content: '' }));
      messages.push({ role: 'user', content: toolResults });
    } else break;
  }
  // Fallback: return last text found
  for (let i = messages.length - 1; i >= 0; i--) {
    if (messages[i].role === 'assistant') {
      const blocks = Array.isArray(messages[i].content) ? messages[i].content : [];
      const text = blocks.filter(b => b.type === 'text').map(b => b.text).join('').trim();
      if (text) return text;
    }
  }
  return '';
}

// ── Merge two contacts ─────────────────────────────────────────
app.post('/api/contacts/merge', (req, res) => {
  try {
  const { keepId, mergeId } = req.body;
  if (!keepId || !mergeId || keepId === mergeId) return res.status(400).json({ error: 'keepId och mergeId krävs och måste vara olika' });
  const keep = db.getContact(keepId);
  const merge = db.getContact(mergeId);
  if (!keep || !merge) return res.status(404).json({ error: 'En eller båda kontakterna saknas' });

  const parseJson = (v, fallback) => { try { return JSON.parse(v || 'null') ?? fallback; } catch { return fallback; } };
  const mergeList = (a, b, key) => {
    const seen = new Set();
    return [...(a || []), ...(b || [])].filter(x => {
      const k = ((key ? x[key] : x) || '').toLowerCase();
      return k && !seen.has(k) && seen.add(k);
    });
  };

  const updates = {};

  // E-post: slå ihop alla adresser, keep är primär
  const keepExtras = parseJson(keep.extra_emails, []);
  const mergeAllEmails = [merge.email, ...parseJson(merge.extra_emails, [])].filter(Boolean);
  const newExtras = [...new Set([...keepExtras, ...mergeAllEmails.filter(e => e !== keep.email)])];
  updates.extra_emails = JSON.stringify(newExtras);

  // Telefon
  const keepPhones = parseJson(keep.extra_phones, []);
  const mergeAllPhones = [merge.phone, ...parseJson(merge.extra_phones, [])].filter(Boolean);
  if (!keep.phone && mergeAllPhones.length) updates.phone = mergeAllPhones.shift();
  const newPhones = [...new Set([...keepPhones, ...mergeAllPhones.filter(p => p !== keep.phone)])];
  updates.extra_phones = JSON.stringify(newPhones);

  // Roller
  const keepRoles = parseJson(keep.roles, []);
  const mergeRoles = parseJson(merge.roles, []);
  if (merge.role) mergeRoles.push(merge.role);
  const allRoles = [...new Set([...keepRoles, ...mergeRoles])].filter(Boolean);
  updates.roles = JSON.stringify(allRoles);

  // Välj det mer kompletta värdet — längre text vinner
  const better = (a, b) => ((b || '').trim().length > (a || '').trim().length) ? b : (a || b || null);
  updates.name = better(keep.name, merge.name);
  if (better(keep.role, merge.role) !== keep.role) updates.role = merge.role;
  if (better(keep.organization, merge.organization) !== keep.organization) updates.organization = merge.organization;
  if (better(keep.notes, merge.notes) !== keep.notes) updates.notes = merge.notes;
  if (!keep.website && merge.website) updates.website = merge.website;
  if (!keep.tags && merge.tags) updates.tags = merge.tags;

  // Enrichment-data: slå ihop, keep har prioritet
  const ke = parseJson(keep.enrichment_data, {});
  const me = parseJson(merge.enrichment_data, {});
  const bio = (ke.bio || '').length >= (me.bio || '').length ? (ke.bio || me.bio || '') : me.bio;
  const mergedEnrich = {
    ...me, ...ke,
    bio,
    productions: mergeList([...(ke.productions || []), ...(me.productions || [])], null, 'title'),
    tags: [...new Set([...(ke.tags || []), ...(me.tags || [])])],
    education: mergeList([...(ke.education || []), ...(me.education || [])], null, 'school'),
    sources: [...new Set([...(ke.sources || []), ...(me.sources || [])])],
    shared_productions: mergeList([...(ke.shared_productions || []), ...(me.shared_productions || [])], null, 'title'),
    shared_education: mergeList([...(ke.shared_education || []), ...(me.shared_education || [])], null, 'school'),
    colleagues: [...new Set([...(ke.colleagues || []), ...(me.colleagues || [])])],
  };
  db.saveEnrichment(keepId, mergedEnrich);

  // Uppdatera kontaktfält
  db.updateContact(keepId, updates);

  // Flytta interaktioner
  db.moveInteractions(mergeId, keepId);

  // Flytta projektkopplingar (hoppa över dubletter)
  const keepProjects = new Set(db.getContactProjects(keepId).map(p => p.id));
  for (const p of db.getContactProjects(mergeId)) {
    if (!keepProjects.has(p.id)) db.linkContactProject(keepId, p.id, p.role_in_project);
  }

  // Flytta org-kopplingar
  const keepOrgs = new Set(db.getContactOrganizations(keepId).map(o => o.id));
  for (const o of db.getContactOrganizations(mergeId)) {
    if (!keepOrgs.has(o.id)) db.linkContactOrganization(keepId, o.id);
  }

  // Flytta foton
  db.movePhotos(mergeId, keepId);

  // Flytta mejl och stipendier
  db.moveEmails(mergeId, keepId);
  db.moveStipends(mergeId, keepId);

  // Ta bort den sammanslagna
  db.deleteContact(mergeId);

  res.json({ ok: true, keepId });
  } catch (err) {
    console.error('[merge]', err);
    res.status(500).json({ error: err.message });
  }
});

// ── AI: hitta dubletter ────────────────────────────────────────
app.post('/api/contacts/find-duplicates', async (req, res) => {
  const contacts = db.getContacts();
  if (contacts.length < 2) return res.json({ groups: [] });

  const list = contacts.map(c =>
    `${c.id}: ${c.name}${c.email ? ' <' + c.email + '>' : ''}${c.organization ? ' · ' + c.organization : ''}${c.role ? ' · ' + c.role : ''}`
  ).join('\n');

  const msg = await anthropic.messages.create({
    model: 'claude-haiku-4-5-20251001', max_tokens: 2000,
    messages: [{ role: 'user', content: `Nedan är en lista med kontakter från ett CRM. Hitta troliga dubletter — kontakter som verkar vara EXAKT samma person.

REGLER:
- Samma domän (@goteborgsstadsteater.se) är INTE en dublett — det är olika personer på samma arbetsplats
- Dublett = samma individ, t.ex. "Anna Svensson" och "Anna K. Svensson" med liknande e-post
- Eller samma e-postadress på två kort
- Var STRIKT — hellre missa en dublett än att flagga olika personer

Kontakter:
${list}

Svara ENBART med JSON-array med grupper av dublett-ID:n:
[{"ids":[1,5],"reason":"Exakt samma namn och e-post"},{"ids":[3,7],"reason":"Samma person, olika stavning av namnet"}]
Om inga uppenbara dubletter finns, svara med [].` }]
  });

  try {
    const m = msg.content[0].text.match(/\[[\s\S]*\]/);
    const groups = JSON.parse(m ? m[0] : '[]');
    // Berika med kontaktdata
    const enriched = groups.map(g => ({
      ...g,
      contacts: g.ids.map(id => contacts.find(c => c.id === id)).filter(Boolean)
    })).filter(g => g.contacts.length > 1);
    res.json({ groups: enriched });
  } catch { res.json({ groups: [] }); }
});

// ── AI: enrich contact ─────────────────────────────────────────
app.post('/api/contacts/:id/enrich', async (req, res) => {
  const contact = db.getContact(req.params.id);
  if (!contact) return res.status(404).json({ error: 'Kontakt saknas' });

  const jonnaContext = getJonnaContext();

  try {
    const jonnaShort = jonnaContext ? jonnaContext.slice(0, 500) : '';

    // Hämta och skrapa hemsida + URL:er från anteckningar
    const scrapeUrl = async (url, label) => {
      try {
        const r = await fetch(url, { headers: { 'User-Agent': 'Mozilla/5.0' }, signal: AbortSignal.timeout(8000) });
        if (!r.ok) return null;
        const html = await r.text();
        const text = html.replace(/<style[\s\S]*?<\/style>/gi, '')
          .replace(/<script[\s\S]*?<\/script>/gi, '')
          .replace(/<[^>]+>/g, ' ')
          .replace(/\s+/g, ' ').trim().slice(0, 2000);
        return text ? `=== ${label} ===\n${text}` : null;
      } catch { return null; }
    };

    // Hitta URL:er i anteckningar
    const notesText = contact.notes || '';
    const urlsInNotes = [...notesText.matchAll(/https?:\/\/[^\s)>\]"]+/g)].map(m => m[0]).slice(0, 3);

    const scrapeResults = await Promise.all([
      contact.website ? scrapeUrl(contact.website, `Hemsida (${contact.website})`) : null,
      ...urlsInNotes.map(u => scrapeUrl(u, `Länk från anteckningar (${u})`))
    ]);
    const scrapedContent = scrapeResults.filter(Boolean).join('\n\n');

    const notesHint = notesText.replace(/https?:\/\/[^\s)>\]"]+/g, '').trim();

    const prompt = `Sök på webben efter information om ${contact.name} inom svensk teater och film. Sök brett på personens NAMN — hela karriären, inte enbart kopplat till en specifik arbetsgivare.${contact.organization ? `\n(Kontext: ${contact.name} är/har varit kopplad till ${contact.organization}, men sök bortom det för att hitta hela karriären.)` : ''}${contact.role ? `\nYrke: ${contact.role}` : ''}${contact.website ? `\nPersonens hemsida: ${contact.website}` : ''}
${notesHint ? `\nAnteckningar om personen (använd som ledtrådar):\n${notesHint}\n` : ''}
${scrapedContent ? `\n${scrapedContent}\n` : ''}
${jonnaShort ? `Jonna (aktörens sammanhang):\n${jonnaShort}\n` : ''}

Gör sökningar och svara sedan ENBART med JSON (inga andra kommentarer):
{
  "bio": "kort sammanfattning (1-2 meningar) om personen",
  "role": "personens yrkestitel, t.ex. regissör / skådespelare / producent",
  "organization": "primär arbetsgivare/teater om känd",
  "tags": ["teater", "film", "regissör"],
  "productions": [{"title": "...", "theater": "...", "year": "...", "role": "..."}],
  "education": [{"school": "Teaterhögskolan i Stockholm", "years": "2001-2004"}],
  "colleagues": ["namn1", "namn2"],
  "sources": ["url1", "url2"]
}

Om personen inte hittas via sökning, använd informationen från hemsidan ovan. Svara ENBART med JSON.`;

    const text = await claudeSearch(prompt, 1500, 'claude-haiku-4-5-20251001', false);
    let enriched;
    try {
      const jsonMatch = text.match(/\{[\s\S]*\}/);
      enriched = JSON.parse(jsonMatch ? jsonMatch[0] : text);
    } catch {
      enriched = { bio: '', role: '', organization: '', tags: [], productions: [], education: [], colleagues: [], sources: [] };
    }

    // Matcha kollegor mot befintliga CRM-kontakter
    const allContacts = db.getContacts();
    const matched_colleagues = (enriched.colleagues || [])
      .map(name => {
        const lower = name.toLowerCase();
        const match = allContacts.find(c => c.id !== contact.id && c.name.toLowerCase().includes(lower));
        return match ? { id: match.id, name: match.name } : null;
      })
      .filter(Boolean);

    // Hitta delade produktioner med Jonna
    const jonnaProfile = db.getJonnaProfile();
    const jonnaProductions = [
      ...(jonnaProfile.cv_parsed?.productions || []),
      ...(jonnaProfile.self_search_results?.productions || [])
    ].map(p => (p.title || '').toLowerCase());

    const shared_productions = (enriched.productions || []).filter(p =>
      jonnaProductions.some(jp => jp && p.title && jp.includes(p.title.toLowerCase()))
    );

    // Slå ihop med befintlig enrichment-data
    const existing = (() => { try { return JSON.parse(contact.enrichment_data || '{}'); } catch { return {}; } })();

    // Bio: behåll längst
    const bio = (enriched.bio || '').length > (existing.bio || '').length ? enriched.bio : (existing.bio || enriched.bio || '');

    // Listor: slå ihop och deduplicera
    const mergeProductions = (a = [], b = []) => {
      const seen = new Set();
      return [...a, ...b].filter(p => { const k = (p.title || '').toLowerCase(); return seen.has(k) ? false : seen.add(k); });
    };
    const mergeTags = (a = [], b = []) => [...new Set([...a, ...b])];
    const mergeEducation = (a = [], b = []) => {
      const seen = new Set();
      return [...a, ...b].filter(e => { const k = (e.school || '').toLowerCase(); return seen.has(k) ? false : seen.add(k); });
    };

    // Slå ihop auto-detekterade shared_productions med manuellt kopplade
    const mergedSharedProductions = mergeProductions(existing.shared_productions || [], shared_productions);

    const toStore = {
      ...existing,
      ...enriched,
      bio,
      productions: mergeProductions(existing.productions, enriched.productions),
      tags: mergeTags(existing.tags, enriched.tags),
      education: mergeEducation(existing.education, enriched.education),
      sources: [...new Set([...(existing.sources || []), ...(enriched.sources || [])])],
      matched_colleagues,
      shared_productions: mergedSharedProductions,
      shared_education: existing.shared_education || [],  // aldrig skriv över manuella skolkopplingar
    };
    db.saveEnrichment(contact.id, toStore);

    // Uppdatera kontaktfält om de är tomma
    const updates = {};
    if (!contact.role && enriched.role) updates.role = enriched.role;
    if (!contact.organization && enriched.organization) updates.organization = enriched.organization;
    if (!contact.tags && enriched.tags?.length) updates.tags = enriched.tags.join(', ');
    if (toStore.education?.length) updates.education = JSON.stringify(toStore.education);
    if (Object.keys(updates).length) db.updateContact(contact.id, updates);

    res.json(toStore);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ── Lägg till skola manuellt på kontakt ───────────────────────
app.post('/api/contacts/:id/add-school', (req, res) => {
  const { school, years } = req.body;
  if (!school) return res.status(400).json({ error: 'school krävs' });
  const contact = db.getContact(req.params.id);
  if (!contact) return res.status(404).json({ error: 'Saknas' });
  const existing = (() => { try { return JSON.parse(contact.enrichment_data || '{}'); } catch { return {}; } })();
  const education = existing.education || [];
  const entry = { school: school.trim(), years: years?.trim() || null };
  if (!education.find(e => (typeof e === 'string' ? e : e.school || '').toLowerCase() === entry.school.toLowerCase())) {
    education.push(entry);
  }
  db.saveEnrichment(req.params.id, { ...existing, education });
  res.json({ ok: true });
});

// ── Kontakt-till-kontakt skolgrupper ──────────────────────────
app.get('/api/contacts/school-groups', (req, res) => {
  const contacts = db.getContacts({});
  const schoolMap = new Map(); // skolnamn (lowercase) → [{id, name, role, photo_url, years}]

  for (const c of contacts) {
    const enrichment = (() => { try { return JSON.parse(c.enrichment_data || '{}'); } catch { return {}; } })();
    const allSchools = [
      ...(enrichment.education || []),
      ...(enrichment.shared_education || []).map(e => ({ school: e.contact_school || e.school, years: e.years })),
    ];
    for (const e of allSchools) {
      const name = (typeof e === 'string' ? e : e.school || '').trim();
      if (!name) continue;
      const key = name.toLowerCase();
      if (!schoolMap.has(key)) schoolMap.set(key, { school: name, contacts: [] });
      const entry = schoolMap.get(key);
      if (!entry.contacts.find(x => x.id === c.id)) {
        const years = typeof e === 'string' ? null : (e.years || null);
        entry.contacts.push({ id: c.id, name: c.name, role: c.role, photo_url: c.photo_url, years });
      }
    }
  }

  const groups = [...schoolMap.values()]
    .filter(g => g.contacts.length >= 2)
    .sort((a, b) => b.contacts.length - a.contacts.length);

  res.json({ groups });
});

// ── Mark school as shared with Jonna ──────────────────────────
app.post('/api/contacts/:id/mark-shared-school', (req, res) => {
  const { school } = req.body;
  if (!school?.school) return res.status(400).json({ error: 'school.school krävs' });
  const contact = db.getContact(req.params.id);
  if (!contact) return res.status(404).json({ error: 'Saknas' });
  const existing = (() => { try { return JSON.parse(contact.enrichment_data || '{}'); } catch { return {}; } })();
  const shared = existing.shared_education || [];
  if (!shared.find(e => (e.school || '').toLowerCase() === (school.school || '').toLowerCase())) {
    shared.push(school);
  }

  // Ta bort skolan ur education-listan — den ska bara finnas under shared_education
  const contactSchool = (school.contact_school || school.school || '').toLowerCase();
  const education = (existing.education || []).filter(e => {
    const s = (typeof e === 'string' ? e : (e.school || '')).toLowerCase();
    return !(s === contactSchool || (s && contactSchool && (s.includes(contactSchool) || contactSchool.includes(s))));
  });

  db.saveEnrichment(req.params.id, { ...existing, shared_education: shared, education });
  res.json({ ok: true });
});

// ── Mark production as shared with Jonna ──────────────────────
app.post('/api/contacts/:id/mark-shared-production', (req, res) => {
  const { production } = req.body;
  if (!production?.title) return res.status(400).json({ error: 'production.title krävs' });
  const contact = db.getContact(req.params.id);
  if (!contact) return res.status(404).json({ error: 'Saknas' });
  const existing = (() => { try { return JSON.parse(contact.enrichment_data || '{}'); } catch { return {}; } })();
  const shared = existing.shared_productions || [];
  if (!shared.find(p => (p.title || '').toLowerCase() === (production.title || '').toLowerCase())) {
    shared.push(production);
    db.saveEnrichment(req.params.id, { ...existing, shared_productions: shared });
  }
  res.json({ ok: true });
});

// ── AI: analyze email history ──────────────────────────────────
app.post('/api/contacts/:id/analyze-emails', async (req, res) => {
  const contact = db.getContact(req.params.id);
  if (!contact) return res.status(404).json({ error: 'Kontakt saknas' });

  const emails = db.getContactEmailsFull(contact.id);
  if (!emails.length) return res.status(400).json({ error: 'Inga mejl kopplade till denna kontakt' });

  // Bygg mejltext — max ~6000 tecken totalt för att hålla nere tokens
  let emailDump = '';
  for (const e of emails) {
    const snippet = (e.body_text || '').slice(0, 400).replace(/\s+/g, ' ');
    emailDump += `\n---\nFrån: ${e.from_address}\nTill: ${e.to_address}\nÄmne: ${e.subject}\nDatum: ${e.received_at}\n${snippet}`;
    if (emailDump.length > 6000) break;
  }

  const jonnaEmails = (() => {
    const profile = db.getJonnaProfile();
    return profile.email || '';
  })();

  const prompt = `Du är ett CRM-system som analyserar mejlkonversationer. Analysera dessa mejl mellan Jonna (${jonnaEmails || 'skådespelaren Jonna'}) och kontakten ${contact.name}.

MEJLHISTORIK:
${emailDump}

Svara ENBART med JSON (inga kommentarer):
{
  "roles": ["roll1"],
  "organization": "primär organisation om känd",
  "phones": ["+46701234567"],
  "emails": ["extra@email.com"],
  "relationship_type": "kort beskrivning av relationstyp, t.ex. 'Potentiell uppdragsgivare', 'Branschkollega', 'Mentor'",
  "relationship_tone": "formell | informell | blandad",
  "initiates_contact": "jonna | kontakten | båda",
  "collab_likelihood": 7,
  "collab_reasoning": "kort motivering (1-2 meningar) varför samarbete är troligt/otroligt",
  "topics": ["teater", "audition"],
  "mentioned_productions": ["Produktionstitel"]
}

Regler:
- Extrahera bara telefonnummer och e-postadresser du FAKTISKT ser i mejlen (t.ex. i signaturer)
- Om du inte hittar information, returnera tom array eller null
- collab_likelihood: 0-10 (10 = mycket troligt)
- Svara ENBART med JSON`;

  try {
    const msg = await anthropic.messages.create({
      model: 'claude-sonnet-4-6',
      max_tokens: 800,
      messages: [{ role: 'user', content: prompt }]
    });
    const text = msg.content.find(b => b.type === 'text')?.text || '';
    let result;
    try {
      const jsonMatch = text.match(/\{[\s\S]*\}/);
      result = JSON.parse(jsonMatch ? jsonMatch[0] : text);
    } catch {
      return res.status(500).json({ error: 'Kunde inte tolka AI-svar' });
    }

    const updates = {};

    // Roller — lägg till nya utan att skriva över
    if (result.roles?.length) {
      const existing = (() => { try { return JSON.parse(contact.roles || '[]'); } catch { return []; } })();
      const merged = [...new Set([...existing, ...result.roles.map(r => r.trim()).filter(Boolean)])];
      if (merged.length > existing.length) updates.roles = JSON.stringify(merged);
    }

    // Telefon — spara i extra_phones om nytt nummer
    if (result.phones?.length) {
      const existingPhones = [contact.phone, ...(() => { try { return JSON.parse(contact.extra_phones || '[]'); } catch { return []; } })()].filter(Boolean).map(p => p.replace(/\s/g, ''));
      const newPhones = result.phones.filter(p => p && !existingPhones.includes(p.replace(/\s/g, '')));
      if (!contact.phone && newPhones.length) {
        updates.phone = newPhones.shift();
      }
      if (newPhones.length) {
        const existingExtra = (() => { try { return JSON.parse(contact.extra_phones || '[]'); } catch { return []; } })();
        updates.extra_phones = JSON.stringify([...existingExtra, ...newPhones]);
      }
    }

    // E-post — lägg till nya i extra_emails
    if (result.emails?.length) {
      const existingEmails = [contact.email, ...(() => { try { return JSON.parse(contact.extra_emails || '[]'); } catch { return []; } })()].filter(Boolean).map(e => e.toLowerCase());
      const newEmails = result.emails.filter(e => e && !existingEmails.includes(e.toLowerCase()));
      if (newEmails.length) {
        const existingExtra = (() => { try { return JSON.parse(contact.extra_emails || '[]'); } catch { return []; } })();
        updates.extra_emails = JSON.stringify([...existingExtra, ...newEmails]);
      }
    }

    // Spara kontaktuppdateringar
    if (Object.keys(updates).length) db.updateContact(contact.id, updates);

    // Lägg till org i contact_organizations om hittad
    if (result.organization) {
      const allOrgs = db.getOrganizations();
      const match = allOrgs.find(o => o.name.toLowerCase().includes(result.organization.toLowerCase()) || result.organization.toLowerCase().includes(o.name.toLowerCase()));
      if (match) {
        const linked = db.getContactOrganizations(contact.id);
        if (!linked.find(o => o.id === match.id)) {
          db.linkContactOrganization(contact.id, match.id);
        }
      }
    }

    // Spara analysresultat i enrichment_data
    const existing = (() => { try { return JSON.parse(contact.enrichment_data || '{}'); } catch { return {}; } })();
    const emailAnalysis = {
      relationship_type: result.relationship_type || null,
      relationship_tone: result.relationship_tone || null,
      initiates_contact: result.initiates_contact || null,
      collab_likelihood: result.collab_likelihood ?? null,
      collab_reasoning: result.collab_reasoning || null,
      topics: result.topics || [],
      mentioned_productions: result.mentioned_productions || [],
      analyzed_at: new Date().toISOString(),
    };
    db.saveEnrichment(contact.id, { ...existing, email_analysis: emailAnalysis });

    // Skapa interaktion med analysresultatet
    const summaryParts = [emailAnalysis.relationship_type];
    if (emailAnalysis.collab_likelihood !== null) summaryParts.push(`samarbetssannolikhet ${emailAnalysis.collab_likelihood}/10`);
    if (emailAnalysis.collab_reasoning) summaryParts.push(emailAnalysis.collab_reasoning);
    const analysisSummary = summaryParts.filter(Boolean).join(' — ');
    if (analysisSummary) {
      db.insertInteraction({
        contact_id: contact.id,
        date: new Date().toISOString().slice(0, 10),
        type: 'AI-analys',
        summary: analysisSummary,
        direction: 'incoming',
      });
    }

    res.json({ ok: true, updates, email_analysis: emailAnalysis });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ── Photos ─────────────────────────────────────────────────────
app.get('/api/contacts/:id/photos', (req, res) => {
  res.json(db.getContactPhotos(req.params.id));
});

app.post('/api/contacts/:id/photos', (req, res) => {
  const { url, data, source } = req.body;
  let photoUrl = url;
  if (data) {
    const base64 = data.replace(/^data:image\/\w+;base64,/, '');
    const ext = data.match(/^data:image\/(\w+);/)?.[1] || 'jpg';
    const filename = `${req.params.id}-${Date.now()}.${ext}`;
    fs.writeFileSync(path.join(uploadsDir, filename), Buffer.from(base64, 'base64'));
    photoUrl = `/uploads/${filename}`;
  }
  if (!photoUrl) return res.status(400).json({ error: 'url eller data krävs' });
  const result = db.addContactPhoto(req.params.id, photoUrl, source || 'manual');
  res.json({ id: result.lastInsertRowid, url: photoUrl });
});

app.delete('/api/photos/:id', (req, res) => {
  db.deleteContactPhoto(req.params.id);
  res.json({ ok: true });
});

app.post('/api/contacts/:id/photos/:photoId/set-primary', (req, res) => {
  db.setPrimaryPhoto(req.params.id, req.params.photoId);
  res.json({ ok: true });
});

// ── AI: hämta profilbild via webbs ökning ──────────────────────
app.post('/api/contacts/:id/fetch-photo', async (req, res) => {
  const contact = db.getContact(req.params.id);
  if (!contact) return res.status(404).json({ error: 'Kontakt saknas' });

  const desc = [contact.name, contact.role, contact.organization].filter(Boolean).join(', ');
  const prompt = `Hitta en profilbild (URL till en JPG/PNG-bild) för: ${desc}.
Sök på t.ex. teaterns hemsida, LinkedIn, IMDB, SVT, Dramaten, svenska kulturinstitutioner.
Svara ENBART med en JSON-rad: {"url":"https://...","source":"var du hittade den"}
Om du inte hittar någon bild, svara: {"url":null,"source":""}`;

  try {
    const text = await claudeSearch(prompt, 500, 'claude-haiku-4-5-20251001', false);
    const m = text.match(/\{[^}]+\}/);
    if (!m) return res.json({ url: null });
    const { url, source } = JSON.parse(m[0]);
    if (!url) return res.json({ url: null });

    // Ladda ner bilden och spara lokalt
    const imgResp = await fetch(url);
    if (!imgResp.ok) return res.json({ url: null });
    const contentType = imgResp.headers.get('content-type') || '';
    if (!contentType.startsWith('image/')) return res.json({ url: null });
    const ext = contentType.includes('png') ? 'png' : 'jpg';
    const filename = `${req.params.id}-ai-${Date.now()}.${ext}`;
    const buffer = Buffer.from(await imgResp.arrayBuffer());
    fs.writeFileSync(path.join(uploadsDir, filename), buffer);
    const photoUrl = `/uploads/${filename}`;

    const result = db.addContactPhoto(req.params.id, photoUrl, source || 'ai');
    db.setPrimaryPhoto(req.params.id, result.lastInsertRowid);
    res.json({ url: photoUrl });
  } catch (err) {
    console.error('[fetch-photo]', err.message);
    res.status(500).json({ error: err.message });
  }
});

// ── Admin: rensa kontakter & mejl ──────────────────────────────
app.post('/api/admin/clear-contacts', (req, res) => {
  db.clearContactsAndEmails();
  res.json({ ok: true });
});

// ── Jonna Profile ──────────────────────────────────────────────
app.get('/api/jonna/profile', (req, res) => {
  const profile = db.getJonnaProfile();
  // Rensa okända avsändare mot befintliga kontakter och Jonnas egna adresser
  if (profile.email_unknown_senders?.length) {
    const contacts = db.getContacts();
    const contactEmails = new Set(contacts.map(c => c.email?.toLowerCase()).filter(Boolean));
    const contactNames = new Set(contacts.map(c => c.name?.toLowerCase()).filter(Boolean));
    // Jonnas egna adresser: IMAP-inloggning + manuellt lagrade alias
    const ownEmails = new Set([
      (profile.email_settings?.user || '').toLowerCase(),
      ...(profile.own_email_aliases || []).map(e => e.toLowerCase()),
    ].filter(Boolean));
    const cleaned = profile.email_unknown_senders.filter(s => {
      if (ownEmails.has(s.address?.toLowerCase())) return false;
      if (contactEmails.has(s.address?.toLowerCase())) return false;
      const localName = (s.name || s.address.split('@')[0]).toLowerCase();
      if (contactNames.has(localName)) return false;
      return true;
    });
    if (cleaned.length !== profile.email_unknown_senders.length) {
      db.setJonnaKey('email_unknown_senders', cleaned);
      profile.email_unknown_senders = cleaned;
    }
  }
  res.json(profile);
});

app.patch('/api/jonna/profile', (req, res) => {
  const { key, value } = req.body;
  if (!key) return res.status(400).json({ error: 'key krävs' });
  db.setJonnaKey(key, value);
  res.json({ ok: true });
});

app.post('/api/jonna/photo', (req, res) => {
  const { data } = req.body;
  if (!data) return res.status(400).json({ error: 'data krävs' });
  const base64 = data.replace(/^data:image\/\w+;base64,/, '');
  const ext = data.match(/^data:image\/(\w+);/)?.[1] || 'jpg';
  const filename = `jonna_photo_${Date.now()}.${ext}`;
  fs.writeFileSync(path.join(uploadsDir, filename), Buffer.from(base64, 'base64'));
  const url = `/uploads/${filename}`;
  db.setJonnaKey('photo', url);
  res.json({ url });
});

app.delete('/api/jonna/photo', (req, res) => {
  const existing = db.getJonnaKey('photo');
  if (existing) {
    const filepath = path.join(__dirname, 'public', existing.replace(/^\//, ''));
    try { fs.unlinkSync(filepath); } catch {}
    db.setJonnaKey('photo', null);
  }
  res.json({ ok: true });
});

app.post('/api/jonna/parse-cv', async (req, res) => {
  const { cv_text } = req.body;
  if (!cv_text) return res.status(400).json({ error: 'cv_text krävs' });
  db.setJonnaKey('cv_raw', cv_text);
  try {
    const msg = await anthropic.messages.create({
      model: 'claude-haiku-4-5-20251001',
      max_tokens: 1500,
      messages: [{
        role: 'user',
        content: `Du är ett system som extraherar strukturerad information från ett CV på svenska.

CV-text:
${cv_text}

Extrahera och svara ENBART med JSON i detta format:
{
  "education": [{"school": "Teaterhögskolan i Malmö", "years": "2001-2004"}, {"school": "Stockholms dramatiska högskola", "years": "1999-2001"}],
  "productions": [{"title": "...", "theater": "...", "year": "...", "role": "..."}],
  "organizations": ["Riksteatern", "Teaterförbundet"]
}

Svara ENBART med JSON.`
      }]
    });
    let parsed;
    try {
      const raw = msg.content[0].text.trim();
      const jsonMatch = raw.match(/\{[\s\S]*\}/);
      parsed = JSON.parse(jsonMatch ? jsonMatch[0] : raw);
    } catch {
      parsed = { education: [], productions: [], organizations: [] };
    }
    db.setJonnaKey('cv_parsed', parsed);
    res.json(parsed);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.post('/api/jonna/letters', (req, res) => {
  const { text } = req.body;
  if (!text) return res.status(400).json({ error: 'text krävs' });
  const letters = db.getJonnaKey('personal_letters') || [];
  const newLetter = { id: Date.now(), text, added_at: new Date().toISOString() };
  letters.push(newLetter);
  db.setJonnaKey('personal_letters', letters);
  res.json(newLetter);
});

app.delete('/api/jonna/letters/:id', (req, res) => {
  const id = Number(req.params.id);
  const letters = db.getJonnaKey('personal_letters') || [];
  db.setJonnaKey('personal_letters', letters.filter(l => l.id !== id));
  res.json({ ok: true });
});

app.post('/api/jonna/analyze-style', async (req, res) => {
  const letters = db.getJonnaKey('personal_letters') || [];

  // Hämta mejl som Jonna skickat
  const ownEmail = db.getJonnaKey('own_email') || (db.getJonnaKey('email_settings') || {}).email || '';
  const sentEmails = ownEmail ? db.getSentEmailSamples(ownEmail, 20) : [];

  let styleAppSamples = [];
  try { styleAppSamples = typeof profile.application_samples === 'string' ? JSON.parse(profile.application_samples) : (profile.application_samples || []); } catch {}
  const artisticStmt = profile.artistic_statement || '';

  if (!letters.length && !sentEmails.length && !styleAppSamples.length) {
    return res.status(400).json({ error: 'Lägg till brev/texter eller importera e-post först' });
  }

  const parts = [];
  if (letters.length) {
    parts.push('=== Personliga brev/texter ===');
    parts.push(letters.map((l, i) => `--- Text ${i + 1} ---\n${l.text}`).join('\n\n'));
  }
  if (sentEmails.length) {
    parts.push('=== Skickade e-post ===');
    parts.push(sentEmails.map((e, i) => `--- Mejl ${i + 1}${e.subject ? ' — ' + e.subject : ''} ---\n${e.body_text || ''}`).join('\n\n'));
  }
  if (styleAppSamples.length) {
    parts.push('=== Exempelansökningar ===');
    parts.push(styleAppSamples.map((s, i) => `--- Ansökan ${i + 1}${s.title ? ' — ' + s.title : ''} ---\n${s.text || ''}`).join('\n\n'));
  }
  if (artisticStmt) {
    parts.push(`=== Konstnärligt statement ===\n${artisticStmt}`);
  }
  const samples = parts.join('\n\n');

  try {
    const msg = await anthropic.messages.create({
      model: 'claude-haiku-4-5-20251001',
      max_tokens: 1000,
      messages: [{
        role: 'user',
        content: `Analysera Jonnas skrivstil baserat på dessa texter för att kunna imitera hennes röst vid AI-genererade ansökningar och meddelanden. Materialet kan innehålla personliga brev, e-post, exempelansökningar och konstnärligt statement.

${samples.slice(0, 12000)}

Svara ENBART med JSON:
{
  "tone": "kort beskrivning av tonen (t.ex. 'varm och direkt')",
  "style_notes": "2-3 meningar om hennes skrivstil, ordval och rytm",
  "example_phrases": ["typisk fras 1", "typisk fras 2", "typisk fras 3"],
  "analysis": "detaljerad analys för AI-instruktioner (3-4 meningar om hur man skriver i hennes stil)"
}`
      }]
    });
    let style;
    try { style = JSON.parse(msg.content[0].text.trim()); }
    catch { style = { tone: '', style_notes: '', example_phrases: [], analysis: '' }; }
    db.setJonnaKey('writing_style', style);
    res.json(style);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.post('/api/jonna/search-self', async (req, res) => {
  const { name, tags } = req.body;
  if (!name) return res.status(400).json({ error: 'name krävs' });
  const searchTags = (tags || ['skådespelerska']).slice(0, 6);
  const jonnaContext = getJonnaContext();

  try {
    const prompt = `Sök på webben efter information om ${name} — en svensk skådespelerska/konstnär. Sök med söktermer som: ${searchTags.join(', ')}.${jonnaContext ? '\n\nKänd bakgrundsinformation om personen (använd för att bekräfta och komplettera):\n' + jonnaContext : ''}

Gör flera sökningar och svara sedan ENBART med JSON:
{
  "bio": "sammanfattning (2-3 meningar) av vem personen är",
  "productions": [{"title": "...", "theater": "...", "year": "...", "role": "...", "source": "url till sidan där produktionen nämns"}],
  "education": [{"school": "...", "years": "..."}],
  "skills": ["mimik", "sång", "rörelsekonst"],
  "sources": ["url1", "url2"]
}

Svara ENBART med JSON.`;

    const text = await claudeSearch(prompt, 2000, 'claude-haiku-4-5-20251001', false);
    let extracted;
    try {
      const jsonMatch = text.match(/\{[\s\S]*\}/);
      extracted = JSON.parse(jsonMatch ? jsonMatch[0] : text);
    } catch {
      extracted = { bio: '', productions: [], education: [], skills: [], sources: [] };
    }

    extracted.searched_at = new Date().toISOString();
    extracted.searched_tags = searchTags;

    // Slå ihop med befintliga resultat istället för att skriva över
    const existing = db.getJonnaKey('self_search_results') || {};
    const existingProds = existing.productions || [];
    const newProds = extracted.productions || [];
    const mergedProds = [...existingProds];
    for (const p of newProds) {
      if (!mergedProds.find(x => (x.title || '').toLowerCase() === (p.title || '').toLowerCase())) {
        mergedProds.push(p);
      }
    }
    const existingEdu = existing.education || [];
    const newEdu = extracted.education || [];
    const mergedEdu = [...existingEdu];
    for (const e of newEdu) {
      if (!mergedEdu.find(x => (x.school || '').toLowerCase() === (e.school || '').toLowerCase())) {
        mergedEdu.push(e);
      }
    }
    const merged = {
      ...existing,
      ...extracted,
      productions: mergedProds,
      education: mergedEdu,
      skills: [...new Set([...(existing.skills || []), ...(extracted.skills || [])])],
      sources: [...new Set([...(existing.sources || []), ...(extracted.sources || [])])],
    };
    db.setJonnaKey('self_search_results', merged);
    if (name) db.setJonnaKey('full_name', name);
    res.json(extracted);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.get('/api/jonna/network/schools', (req, res) => {
  const cvParsed = db.getJonnaKey('cv_parsed');
  const manualEdu = db.getJonnaKey('manual_education') || [];
  const selfSearch = db.getJonnaKey('self_search_results');
  const rawEdu = [...(cvParsed?.education || []), ...manualEdu, ...(selfSearch?.education || [])];
  // Normalize to {school, years} regardless of format
  const seen = new Set();
  const schools = rawEdu.map(e =>
    typeof e === 'string' ? { school: e.replace(/\s*\d{4}.*$/, '').trim(), years: e } : e
  ).filter(s => {
    if (!s.school) return false;
    if (seen.has(s.school)) return false;
    seen.add(s.school);
    return true;
  });
  if (!schools.length) return res.json({ schools: [], connections: [] });
  const connections = db.getSchoolConnections(schools);
  res.json({ schools, connections });
});

// ── Discover: Job listings ─────────────────────────────────────
app.get('/api/discover/jobs', (req, res) => {
  const contacts = db.getContacts({});
  const jobs = db.getJobListings();
  for (const j of jobs) {
    const existing = (() => { try { return JSON.parse(j.known_contacts || '[]'); } catch { return []; } })();
    const existingIds = new Set(existing.map(c => c.id));
    // Lägg till nya kontakter som inte matchades när jobbet skapades
    const updated = [...existing];
    let changed = false;
    for (const c of contacts) {
      if (!existingIds.has(c.id)) {
        const lower = c.name.toLowerCase();
        // Kolla om kontaktens namn nämns i titel, org eller beskrivning
        const haystack = `${j.title} ${j.organization} ${j.description}`.toLowerCase();
        if (haystack.includes(lower)) {
          updated.push({ id: c.id, name: c.name });
          changed = true;
        }
      }
    }
    if (changed) {
      db.updateJobKnownContacts(j.id, JSON.stringify(updated));
      j.known_contacts = updated;
    } else {
      j.known_contacts = existing;
    }
  }
  res.json(jobs);
});

// ── Scraper: hämta jobblistningar från gratissajter ────────────
const SCRAPE_SITES = [
  { url: 'https://www.kulturjobb.se/', name: 'kulturjobb.se' },
  { url: 'https://www.teateralliansen.se/lediga-jobb', name: 'teateralliansen.se' },
  { url: 'https://www.sceneochfilm.se/jobb', name: 'sceneochfilm.se' },
  { url: 'https://filmcafe.se/jobb/', name: 'filmcafe.se' },
  { url: 'https://sv.stagepool.com/skadespelare/102300/skadespelare_sokes_till_oppen_casting', name: 'stagepool.com' },
];

const SCRAPE_STIPEND_SITES = [
  { url: 'https://www.konstnärsnämnden.se/stipendier-och-bidrag/', name: 'Konstnärsnämnden' },
  { url: 'https://www.kulturradet.se/bidrag/', name: 'Kulturrådet' },
  { url: 'https://www.filminstitutet.se/finansiering/', name: 'Svenska Filminstitutet' },
  { url: 'https://stim.se/stod-och-stipendier/', name: 'STIM' },
];

async function scrapeSite({ url, name }) {
  try {
    const res = await fetch(url, {
      headers: { 'User-Agent': 'Mozilla/5.0 (compatible; ActressCRM/1.0)' },
      signal: AbortSignal.timeout(8000)
    });
    if (!res.ok) return null;
    const html = await res.text();
    const $ = cheerio.load(html);
    $('script, style, nav, footer, header, form, .cookie-banner, .ads').remove();
    const text = $('main, article, .jobs, .listings, #content, body')
      .first().text().replace(/\s+/g, ' ').trim().slice(0, 4000);
    return text ? `=== ${name} ===\n${text}` : null;
  } catch (e) {
    console.warn(`[scraper] ${name}: ${e.message}`);
    return null;
  }
}

async function scrapeJobSites() {
  const results = await Promise.all(SCRAPE_SITES.map(scrapeSite));
  return results.filter(Boolean).join('\n\n');
}

async function scrapeStipendSites() {
  const results = await Promise.all(SCRAPE_STIPEND_SITES.map(scrapeSite));
  return results.filter(Boolean).join('\n\n');
}

app.post('/api/discover/jobs/search', async (req, res) => {
  const jonnaProfile = db.getJonnaProfile();
  const extraSites = jonnaProfile.discover_sites_jobs || [];

  try {
    const existingTitles = db.getJobTitles();
    const contacts = db.getContacts();

    const [scrapedContent] = await Promise.all([scrapeJobSites()]);
    const alreadyKnown = existingTitles.length ? `\nDessa har redan hittats — hitta ANDRA: ${existingTitles.slice(0, 8).join(', ')}` : '';
    const scrapedSection = scrapedContent ? `\n\nHär är scrapad data från jobbannonssajter — använd detta som primär källa:\n${scrapedContent}` : '';
    const extraSection = extraSites.length ? `\n\nSök SPECIFIKT även på dessa sidor: ${extraSites.join(', ')}` : '';

    const prompt = `Du ska hitta aktuella jobbannonser för skådespelare och scenkonst i Sverige.${scrapedSection}${extraSection}${alreadyKnown}

Om sökanden: ${getJonnaSearchSummary()}

Extrahera annonser ur den scrapade texten ovan och komplettera med websökning om det behövs. Hitta 15-20 relevanta annonser/möjligheter. Svara ENBART med JSON-array:
[{
  "title": "Rollnamn eller tjänstetitel",
  "organization": "Teater/produktionsbolag",
  "description": "Kort beskrivning av rollen/uppdraget (2-3 meningar)",
  "url": "länk till annonsen",
  "deadline": "sista ansökningsdag om känd, annars null",
  "interesting_score": 4,
  "interesting_reason": "Varför detta är relevant för Jonna (1 mening)",
  "people_mentioned": ["namn på personer nämnda i annonsen om några"]
}]

Intressant score 1-5 där 5 = perfekt match för Jonna. Svara ENBART med JSON-array.`;

    const text = await claudeSearch(prompt, 5000);
    let listings = [];
    try {
      const jsonMatch = text.match(/\[[\s\S]*\]/);
      listings = JSON.parse(jsonMatch ? jsonMatch[0] : '[]');
    } catch { listings = []; }

    const saved = [];
    if (listings.length > 0) {
      db.clearJobListings();
      for (const l of listings) {
        const knownContacts = (l.people_mentioned || [])
          .map(name => {
            const lower = name.toLowerCase();
            const match = contacts.find(c => c.name.toLowerCase().includes(lower));
            return match ? { id: match.id, name: match.name } : null;
          })
          .filter(Boolean);
        db.insertJobListing({ ...l, known_contacts: JSON.stringify(knownContacts) });
        saved.push({ ...l, known_contacts: knownContacts });
      }
    }

    if (saved.length) db.setJonnaKey('last_searched_jobs', new Date().toISOString());
    res.json(saved.length ? saved : db.getJobListings?.() || []);
  } catch (err) {
    const status = err.status === 529 ? 529 : err.status === 429 ? 429 : 500;
    const msg = err.status === 529 ? 'API:et är tillfälligt överbelastat — försök igen om en stund.'
              : err.status === 429 ? 'För många förfrågningar — vänta en minut och försök igen.'
              : err.message;
    res.status(status).json({ error: msg });
  }
});

app.patch('/api/discover/jobs/:id/save', (req, res) => {
  db.saveJobListing(req.params.id, req.body.saved);
  if (req.body.saved) {
    const job = db.getJobById(req.params.id);
    const knownContacts = (() => { try { return JSON.parse(job?.known_contacts || '[]'); } catch { return []; } })();
    for (const c of knownContacts) {
      db.insertInteraction({
        contact_id: c.id,
        date: new Date().toISOString().slice(0, 10),
        type: 'Jobbannons',
        summary: `Nämns i annons: "${job.title}"${job.organization ? ' hos ' + job.organization : ''}`,
        direction: 'incoming',
      });
    }
  }
  res.json({ ok: true });
});

app.patch('/api/discover/jobs/:id/feedback', (req, res) => {
  db.feedbackJob(req.params.id, req.body.feedback);
  res.json({ ok: true });
});

// ── Kontaktens stipendier ──────────────────────────────────────
app.get('/api/contacts/:id/stipends', (req, res) => {
  res.json(db.getContactStipends(req.params.id));
});

app.post('/api/contacts/:id/stipends/link', (req, res) => {
  const { stipend_id } = req.body;
  db.linkStipendToContact(stipend_id, req.params.id);
  res.json({ ok: true });
});

// ── Discover: Stipends ─────────────────────────────────────────
app.get('/api/discover/stipends', (req, res) => {
  // Uppdatera matchning mot aktuell kontaktlista innan vi returnerar
  const contacts = db.getContacts({});
  const stipends = db.getStipendFindings();
  for (const s of stipends) {
    const lower = (s.person_name || '').toLowerCase();
    const match = lower ? contacts.find(c => c.name.toLowerCase().includes(lower) || lower.includes(c.name.toLowerCase())) : null;
    const newId = match?.id || null;
    if (newId !== s.matched_contact) {
      db.updateStipendContact(s.id, newId);
      s.matched_contact = newId;
      s.contact_name = match?.name || null;
    }
  }
  res.json(stipends);
});

app.post('/api/discover/stipends/search', async (req, res) => {
  const jonnaProfile = db.getJonnaProfile();
  const extraSites = jonnaProfile.discover_sites_stipends || [];
  try {
    if (req.query.reset === 'true') db.clearStipendFindings();
    const existingNames = db.getStipendNames();
    const existingKeys = db.getStipendKeys();
    const existingKeySet = new Set(existingKeys.map(r =>
      `${(r.person_name || '').toLowerCase()}|${(r.organization || '').toLowerCase()}|${(r.year || '')}`
    ));
    const contacts = db.getContacts();

    const scrapedContent = await scrapeStipendSites();
    const scrapedSection = scrapedContent ? `\n\nHär är scrapad data från stipendiesajter — använd detta som primär källa:\n${scrapedContent}` : '';
    const extraSection = extraSites.length ? `\n\nSök SPECIFIKT även på dessa sidor: ${extraSites.join(', ')}` : '';
    const alreadyKnown = existingNames.length ? `\nDessa finns redan i biblioteket — hitta ANDRA personer: ${existingNames.join(', ')}` : '';

    const prompt = `Du ska hitta personer som nyligen fått stipendier eller bidrag från svenska kulturinstitutioner inom teater och scenkonst.${scrapedSection}${extraSection}${alreadyKnown}

Komplettera med websökning mot: Stiftelsen Längmanska kulturfonden, Helge Ax:son Johnsons stiftelse, regionala kulturnämnder, enskilda teaters stipendiefonder.

Extrahera stipendiater ur den scrapade texten och komplettera med websökning. Hitta 20-25 stipendiater. Svara ENBART med JSON-array:
[{
  "person_name": "Förnamn Efternamn",
  "organization": "Konstnärsnämnden",
  "year": "2024",
  "description": "Vad stipendiet gäller och vad personen arbetar med (1-2 meningar)",
  "url": "länk till källan"
}]

Svara ENBART med JSON-array.`;

    const text = await claudeSearch(prompt, 3000);
    let findings = [];
    try {
      const jsonMatch = text.match(/\[[\s\S]*\]/);
      findings = JSON.parse(jsonMatch ? jsonMatch[0] : '[]');
    } catch { findings = []; }

    const newFindings = [];
    for (const f of findings) {
      const lower = (f.person_name || '').toLowerCase();
      if (!lower) continue;
      const key = `${lower}|${(f.organization || '').toLowerCase()}|${(f.year || '')}`;
      if (existingKeySet.has(key)) continue; // samma person + org + år finns redan
      const match = lower ? contacts.find(c => c.name.toLowerCase() === lower) : null;
      db.insertStipendFinding({ ...f, matched_contact: match?.id || null });
      newFindings.push({ ...f, matched_contact: match?.id || null, contact_name: match?.name || null });
    }

    // Returnera alla stipendiater (biblioteket) för visning
    const allStipends = db.getStipendFindings();
    for (const s of allStipends) {
      const lower = (s.person_name || '').toLowerCase();
      const match = lower ? contacts.find(c => c.name.toLowerCase() === lower) : null;
      if ((match?.id || null) !== s.matched_contact) {
        db.prepare('UPDATE stipend_findings SET matched_contact = ? WHERE id = ?').run(match?.id || null, s.id);
        s.matched_contact = match?.id || null;
        s.contact_name = match?.name || null;
      }
    }

    if (newFindings.length) db.setJonnaKey('last_searched_stipends', new Date().toISOString());
    res.json(allStipends);
  } catch (err) {
    const status = err.status === 529 ? 529 : err.status === 429 ? 429 : 500;
    const msg = err.status === 529 ? 'API:et är tillfälligt överbelastat — försök igen om en stund.'
              : err.status === 429 ? 'För många förfrågningar — vänta en minut och försök igen.'
              : err.message;
    res.status(status).json({ error: msg });
  }
});

app.patch('/api/discover/stipends/:id/save', (req, res) => {
  db.saveStipendFinding(req.params.id, req.body.saved);
  if (req.body.saved) {
    const stipend = db.getStipendById(req.params.id);
    if (stipend?.matched_contact) {
      db.insertInteraction({
        contact_id: stipend.matched_contact,
        date: stipend.year ? `${stipend.year}-01-01` : new Date().toISOString().slice(0, 10),
        type: 'Stipendium',
        summary: `Fick stipendium från ${stipend.organization}${stipend.description ? ': ' + stipend.description.slice(0, 120) : ''}`,
        direction: 'incoming',
      });
    }
  }
  res.json({ ok: true });
});

app.patch('/api/discover/stipends/:id/unlink', (req, res) => {
  db.prepare('UPDATE stipend_findings SET matched_contact = NULL WHERE id = ?').run(req.params.id);
  res.json({ ok: true });
});

app.patch('/api/discover/stipends/:id/feedback', (req, res) => {
  db.feedbackStipend(req.params.id, req.body.feedback);
  res.json({ ok: true });
});

// ── Discover: Grant calls ──────────────────────────────────────
app.get('/api/discover/grants', (req, res) => {
  res.json(db.getGrantCalls());
});

app.post('/api/discover/grants/search', async (req, res) => {
  const jonnaProfile = db.getJonnaProfile();
  const fullContext = getJonnaContext();

  try {
    const extraSites = jonnaProfile.discover_sites_grants || [];
    const existingTitles = db.getGrantTitles();

    const alreadyKnown = existingTitles.length ? `\nDessa bidrag har redan hittats — hitta ANDRA: ${existingTitles.slice(0, 8).join(', ')}` : '';
    const prompt = `Sök efter öppna ansökningar för projektbidrag, produktionsstöd och arbetsstipendier för scenkonstnärer och skådespelare i Sverige. Sök på kulturradet.se, konstnarsnamnden.se och regionala kulturnämnders webbplatser (Västra Götaland, Stockholm, Skåne).${extraSites.length ? '\n\nSök SPECIFIKT även på dessa sidor: ' + extraSites.join(', ') : ''}${alreadyKnown}

Hitta 8-10 öppna bidrag och stipendier. Svara ENBART med JSON-array:
[{
  "title": "Bidragets namn",
  "organization": "Utgivande organisation",
  "description": "Vad bidraget är till för och varför det passar Jonna (2-3 meningar)",
  "url": "länk",
  "deadline": "sista ansökningsdag eller null",
  "amount": "belopp om känt, annars null",
  "match_reason": "Kort motivering varför detta passar Jonna specifikt (1 mening)"
}]

Svara ENBART med JSON-array.`;

    const text = await claudeSearch(prompt, 5000);
    let grants = [];
    try {
      const jsonMatch = text.match(/\[[\s\S]*\]/);
      grants = JSON.parse(jsonMatch ? jsonMatch[0] : '[]');
    } catch (e) {
      console.error('[grants] JSON-parse misslyckades:', e.message, '\nText (500 tecken):', text?.slice(0, 500));
      grants = [];
    }
    if (!grants.length) console.warn('[grants] Inga bidrag hittades. Rå text:', text?.slice(0, 300));

    // Rensa bara om vi faktiskt fick nya resultat — annars behåll gamla
    if (grants.length > 0) {
      db.clearGrantCalls();
      for (const g of grants) db.insertGrantCall(g);
      db.setJonnaKey('last_searched_grants', new Date().toISOString());
    }
    res.json(db.getGrantCalls());
  } catch (err) {
    const status = err.status === 529 ? 529 : err.status === 429 ? 429 : 500;
    const msg = err.status === 529 ? 'API:et är tillfälligt överbelastat — försök igen om en stund.'
              : err.status === 429 ? 'För många förfrågningar — vänta en minut och försök igen.'
              : err.message;
    res.status(status).json({ error: msg });
  }
});

app.patch('/api/discover/grants/:id/save', (req, res) => {
  db.saveGrantCall(req.params.id, req.body.saved);
  res.json({ ok: true });
});

app.patch('/api/discover/grants/:id/feedback', (req, res) => {
  db.feedbackGrant(req.params.id, req.body.feedback);
  res.json({ ok: true });
});

// ── Emails per contact ─────────────────────────────────────────
app.get('/api/contacts/:id/emails', (req, res) => {
  res.json(db.getContactEmails(req.params.id));
});

// ── Email sender feedback (inlärning) ─────────────────────────
app.get('/api/emails/sender-feedback', (req, res) => {
  res.json(db.getJonnaKey('email_sender_feedback') || []);
});

app.post('/api/emails/sender-feedback', (req, res) => {
  const { address, name, decision, reason } = req.body; // decision: 'approved' | 'rejected'
  if (!address || !decision) return res.status(400).json({ error: 'address och decision krävs' });
  const feedback = db.getJonnaKey('email_sender_feedback') || [];
  // Ersätt om samma adress redan finns
  const idx = feedback.findIndex(f => f.address === address);
  const entry = { address, name: name || '', decision, reason: reason || '', saved_at: new Date().toISOString() };
  if (idx >= 0) feedback[idx] = entry; else feedback.push(entry);
  db.setJonnaKey('email_sender_feedback', feedback.slice(-100)); // max 100 poster
  // Vid avvisning — ta bort från den sparade listan så de inte kommer tillbaka vid sidladdning
  if (decision === 'rejected') {
    const senders = db.getJonnaKey('email_unknown_senders') || [];
    db.setJonnaKey('email_unknown_senders', senders.filter(s => s.address !== address));
  }
  res.json({ ok: true });
});

// ── CSV-export av okända avsändare ────────────────────────────
app.get('/api/emails/unknown-senders/export.csv', (req, res) => {
  const senders = db.getJonnaKey('email_unknown_senders') || [];
  const headers = ['Namn', 'E-post', 'Intressant?'];
  const escape = v => `"${String(v ?? '').replace(/"/g, '""')}"`;
  const rows = senders.map(s => [
    s.name && s.name !== s.address ? s.name : s.address.split('@')[0],
    s.address,
    '',
  ].map(escape).join(','));

  const csv = '\uFEFF' + [headers.map(escape).join(','), ...rows].join('\r\n');
  res.setHeader('Content-Type', 'text/csv; charset=utf-8');
  res.setHeader('Content-Disposition', 'attachment; filename="okanda-avsandare.csv"');
  res.send(csv);
});

// ── CSV-import: synka Jonnas urval ────────────────────────────
app.post('/api/emails/unknown-senders/import-csv', async (req, res) => {
  const text = req.body?.toString?.() || '';
  const clean = text.replace(/^\uFEFF/, '').replace(/\r\n/g, '\n').replace(/\r/g, '\n');
  const lines = clean.split('\n').filter(l => l.trim());
  if (lines.length < 2) return res.json({ added: 0, skipped: 0 });

  function parseCSVLine(line) {
    const result = [];
    let cur = '', inQ = false;
    for (let i = 0; i < line.length; i++) {
      const c = line[i];
      if (c === '"') { if (inQ && line[i + 1] === '"') { cur += '"'; i++; } else inQ = !inQ; }
      else if ((c === ',' || c === ';') && !inQ) { result.push(cur); cur = ''; }
      else cur += c;
    }
    result.push(cur);
    return result;
  }

  const sep = lines[0].includes(';') ? ';' : ',';
  const headerLine = lines[0].replace(/;/g, ',');
  const headers = parseCSVLine(headerLine).map(h => h.trim().toLowerCase().replace(/"/g, ''));
  const emailIdx   = headers.findIndex(h => h.includes('e-post') || h === 'email');
  const nameIdx    = headers.findIndex(h => h === 'namn' || h === 'name');
  const roleIdx    = headers.findIndex(h => h.includes('roll'));
  const interestIdx = headers.findIndex(h => h.includes('intressant'));

  if (emailIdx === -1) return res.status(400).json({ error: 'Ingen e-postkolumn hittad i CSV' });

  const contacts = db.getContacts();
  const existingEmails = new Set(contacts.map(c => c.email?.toLowerCase()).filter(Boolean));

  const dataRows = lines.length - 1;
  console.log(`[csv-import] Startar — ${dataRows} rader, kolumner: namn=${nameIdx >= 0 ? 'ja' : 'nej'}, roll=${roleIdx >= 0 ? 'ja' : 'nej'}, intressant=${interestIdx >= 0 ? 'ja' : 'nej'}`);

  let added = 0, skipped = 0;
  for (let i = 1; i < lines.length; i++) {
    const cells = parseCSVLine(lines[i].replace(/;/g, ',')).map(c => c.trim().replace(/^"|"$/g, ''));
    const email = (cells[emailIdx] || '').toLowerCase().trim();
    if (!email) { skipped++; continue; }

    const interesting = interestIdx >= 0 ? cells[interestIdx] || '' : '';
    const checked = ['sant', 'true', 'ja', 'yes', 'x', '1'].includes(interesting.toLowerCase());

    if (!checked) {
      // Okryssad rad — lämna ifred, rör inte okändlistan
      skipped++;
      continue;
    }

    if (existingEmails.has(email)) {
      console.log(`[csv-import] Hoppar över (finns redan): ${email}`);
      skipped++;
      continue;
    }

    const name = (nameIdx >= 0 && cells[nameIdx]) ? cells[nameIdx] : email.split('@')[0];
    const role = roleIdx >= 0 ? cells[roleIdx] || null : null;

    const result = db.insertContact({ name, email, role });
    const contactId = result.lastInsertRowid;
    existingEmails.add(email);

    // Markera som godkänd och ta bort från okändlistan
    const feedback = db.getJonnaKey('email_sender_feedback') || [];
    const idx = feedback.findIndex(f => f.address === email);
    const entry = { address: email, name, decision: 'approved', reason: 'Via CSV-import', saved_at: new Date().toISOString() };
    if (idx >= 0) feedback[idx] = entry; else feedback.push(entry);
    db.setJonnaKey('email_sender_feedback', feedback.slice(-100));

    const senders = db.getJonnaKey('email_unknown_senders') || [];
    db.setJonnaKey('email_unknown_senders', senders.filter(s => s.address !== email));

    // Koppla befintliga mejl till kontakten
    let linkedEmails = 0;
    try {
      const emails = db.findEmailsByAddress(email, contactId);
      for (const e of emails) {
        db.matchEmail(e.id, contactId);
        const existing = db.getInteractionBySubject(contactId, e.subject || '');
        if (!existing && e.subject) {
          const dir = e.from_address?.toLowerCase().includes(email) ? 'incoming' : 'outgoing';
          db.insertInteraction({ contact_id: contactId, date: e.received_at?.slice(0, 10) || new Date().toISOString().slice(0, 10), type: 'E-post', summary: e.subject, direction: dir });
        }
        linkedEmails++;
      }
    } catch {}

    console.log(`[csv-import] + ${name} <${email}>${role ? ` (${role})` : ''}${linkedEmails ? ` — ${linkedEmails} mejl kopplade` : ''}`);
    added++;
  }

  console.log(`[csv-import] Klar — ${added} tillagda, ${skipped} hoppades över`);
  res.json({ added, skipped });
});

app.post('/api/emails/sender-feedback/undo', (req, res) => {
  const { address } = req.body;
  if (!address) return res.status(400).json({ error: 'address krävs' });
  // Ta bort från feedback
  const feedback = db.getJonnaKey('email_sender_feedback') || [];
  const entry = feedback.find(f => f.address === address);
  db.setJonnaKey('email_sender_feedback', feedback.filter(f => f.address !== address));
  // Lägg tillbaka i unknown_senders
  if (entry) {
    const senders = db.getJonnaKey('email_unknown_senders') || [];
    if (!senders.find(s => s.address === address)) {
      senders.unshift({ address, name: entry.name, count: 1 });
      db.setJonnaKey('email_unknown_senders', senders);
    }
  }
  res.json({ ok: true });
});

// ── Email import (mbox) ────────────────────────────────────────

app.post('/api/emails/import-mbox', async (req, res) => {
  const tmpFile = path.join(os.tmpdir(), `actress-mbox-${Date.now()}.tmp`);
  try {
  const years = parseInt(req.query.years) || 0;
  const filename = req.query.filename || 'okänd fil';

  console.log(`[mbox] Import startar: "${filename}", sparar till disk...`);

  // 1. Pipe request body straight to disk — no RAM used
  await new Promise((resolve, reject) => {
    const ws = fs.createWriteStream(tmpFile);
    req.pipe(ws);
    ws.on('finish', resolve);
    ws.on('error', reject);
    req.on('error', reject);
  });

  const fileSizeMb = (fs.statSync(tmpFile).size / 1024 / 1024).toFixed(1);
  console.log(`[mbox] ${fileSizeMb} MB sparade till disk, bearbetar rad för rad...`);

  const cutoff = years ? new Date(Date.now() - years * 365.25 * 24 * 60 * 60 * 1000) : null;
  const contacts = db.getContacts();
  const organizations = db.getOrganizations();
  const ownEmails = new Set([
    (db.getJonnaKey('email_settings')?.user || '').toLowerCase(),
    ...(db.getJonnaKey('own_email_aliases') || []).map(e => e.toLowerCase()),
  ].filter(Boolean));

  let imported = 0;
  let matched = 0;
  let total = 0;
  const unknownMap = new Map();

  // 2. Process line by line — one message in memory at a time
  const processMessage = async (lines) => {
    if (!lines.length) return;
    total++;
    try {
      const parsed = await simpleParser(lines.join('\n'));
      const msgId = parsed.messageId || `mbox-${Date.now()}-${Math.random()}`;
      const fromAddr = parsed.from?.value?.[0]?.address?.toLowerCase() || '';
      const fromName = parsed.from?.value?.[0]?.name || fromAddr;
      const fromText = parsed.from?.text || '';
      const toText = parsed.to?.text || '';
      const subject = parsed.subject || '';
      const bodyText = (parsed.text || '').slice(0, 2000);
      const date = parsed.date?.toISOString() || new Date().toISOString();

      if (cutoff && parsed.date && parsed.date < cutoff) { total--; return; }

      const allAddresses = [
        fromAddr,
        ...(parsed.to?.value || []).map(a => a.address?.toLowerCase()).filter(Boolean),
        ...(parsed.cc?.value || []).map(a => a.address?.toLowerCase()).filter(Boolean),
      ];

      let matchedContact = null;
      for (const c of contacts) {
        const extras = (() => { try { return JSON.parse(c.extra_emails || '[]'); } catch { return []; } })();
        const allContactEmails = [c.email, ...extras].filter(Boolean).map(e => e.toLowerCase());
        if (allContactEmails.some(e => allAddresses.includes(e))) { matchedContact = c.id; break; }
      }

      let matchedOrganization = null;
      for (const o of organizations) {
        const orgEmails = (o.generic_emails || []).map(e => e.toLowerCase());
        if (orgEmails.some(e => allAddresses.includes(e))) { matchedOrganization = o.id; break; }
        if (!matchedOrganization && o.domain) {
          const dom = o.domain.toLowerCase();
          if (allAddresses.some(a => a.endsWith('@' + dom))) { matchedOrganization = o.id; break; }
        }
      }

      const localPart = fromAddr ? fromAddr.split('@')[0] : '';
      const isAutoSender = /^(noreply|no-reply|no\.reply|donotreply|do-not-reply)/.test(localPart);
      if (!matchedContact && fromAddr && !ownEmails.has(fromAddr) && !isAutoSender) {
        if (!unknownMap.has(fromAddr)) unknownMap.set(fromAddr, { address: fromAddr, name: fromName, count: 0, subjects: [] });
        const entry = unknownMap.get(fromAddr);
        entry.count++;
        if (subject && entry.subjects.length < 5) entry.subjects.push(subject);
      }

      const result = db.insertEmail({ message_id: msgId, from_address: fromText, to_address: toText, subject, body_text: bodyText, received_at: date, matched_contact: matchedContact, matched_organization: matchedOrganization });
      if (result.changes) { imported++; if (matchedContact) matched++; }
    } catch {}
  };

  const rl = readline.createInterface({
    input: fs.createReadStream(tmpFile, { encoding: 'utf8' }),
    crlfDelay: Infinity
  });

  let msgLines = [];
  for await (const line of rl) {
    if (/^From \S/.test(line) && msgLines.length > 0) {
      await processMessage(msgLines);
      msgLines = [line];
    } else {
      msgLines.push(line);
    }
  }
  await processMessage(msgLines); // last message

  const unknownSenders = [...unknownMap.values()].sort((a, b) => b.count - a.count);

  // AI-analys av okända avsändare i omgångar — ingen försvinner pga gräns
  if (unknownSenders.length) {
    try {
      const analysis = await filterEmailSendersBatched(unknownSenders);
      db.setJonnaKey('email_unknown_senders', applyAnalysis(unknownSenders, analysis));
    } catch { db.setJonnaKey('email_unknown_senders', unknownSenders); }
  }

  // Spara importhistorik
  const history = db.getJonnaKey('email_import_history') || [];
  history.unshift({ filename, imported, matched, total, imported_at: new Date().toISOString() });
  db.setJonnaKey('email_import_history', history.slice(0, 50)); // max 50 poster

  console.log(`[mbox] Klar: ${imported} importerade, ${matched} matchade, ${total} totalt`);
  res.json({ imported, matched, total });
  } catch (err) {
    console.error('[mbox] Krasch:', err);
    if (!res.headersSent) res.status(500).json({ error: err.message || 'Serverfel' });
  } finally {
    fs.unlink(tmpFile, () => {}); // städa bort tempfil
  }
});

// ── Email sync (IMAP) ──────────────────────────────────────────
app.post('/api/emails/sync', async (req, res) => {
  const settings = db.getJonnaKey('email_settings');
  if (!settings?.user || !settings?.pass) {
    return res.status(400).json({ error: 'E-postinställningar saknas. Lägg till i Min profil → E-post.' });
  }

  const user = settings.user.toLowerCase();
  const host = user.endsWith('@hotmail.com') || user.endsWith('@hotmail.se') || user.endsWith('@live.com') || user.endsWith('@msn.com')
    ? 'imap-mail.outlook.com'
    : 'outlook.office365.com';

  const client = new ImapFlow({
    host,
    port: 993,
    secure: true,
    auth: { user: settings.user, pass: settings.pass },
    logger: false,
    tls: { rejectUnauthorized: false }
  });

  try {
    await client.connect();
    const lock = await client.getMailboxLock('INBOX');
    try {
      const lastSync = db.getJonnaKey('email_last_sync');
      const since = lastSync ? new Date(lastSync) : new Date(Date.now() - 90 * 24 * 60 * 60 * 1000);

      const contacts = db.getContacts();
      const organizations = db.getOrganizations();
      let synced = 0;
      let matched = 0;
      const unknownMap = new Map();

      const uids = await client.search({ since }, { uid: true });
      const batch = uids.slice(-200); // latest 200

      for await (const msg of client.fetch(batch, { uid: true, source: true }, { uid: true })) {
        try {
          const parsed = await simpleParser(msg.source);
          const msgId = parsed.messageId || `uid-${msg.uid}`;
          const fromAddr = parsed.from?.value?.[0]?.address?.toLowerCase() || '';
          const fromName = parsed.from?.value?.[0]?.name || fromAddr;
          const fromText = parsed.from?.text || '';
          const toText = parsed.to?.text || '';
          const subject = parsed.subject || '';
          const bodyText = (parsed.text || '').slice(0, 2000);
          const date = parsed.date?.toISOString() || new Date().toISOString();

          const allAddresses = [
            fromAddr,
            ...(parsed.to?.value || []).map(a => a.address?.toLowerCase()).filter(Boolean),
            ...(parsed.cc?.value || []).map(a => a.address?.toLowerCase()).filter(Boolean),
          ];

          let matchedContact = null;
          for (const c of contacts) {
            if (c.email && allAddresses.includes(c.email.toLowerCase())) {
              matchedContact = c.id;
              break;
            }
          }

          let matchedOrganization = null;
          for (const o of organizations) {
            const orgEmails = (o.generic_emails || []).map(e => e.toLowerCase());
            if (orgEmails.some(e => allAddresses.includes(e))) { matchedOrganization = o.id; break; }
            if (!matchedOrganization && o.domain) {
              const dom = o.domain.toLowerCase();
              if (allAddresses.some(a => a.endsWith('@' + dom))) { matchedOrganization = o.id; break; }
            }
          }

          const ownEmailsImap = new Set([
            settings.user.toLowerCase(),
            ...(db.getJonnaKey('own_email_aliases') || []).map(e => e.toLowerCase()),
          ].filter(Boolean));
          const localPart2 = fromAddr ? fromAddr.split('@')[0] : '';
          const isAutoSender2 = /^(noreply|no-reply|no\.reply|donotreply|do-not-reply)/.test(localPart2);
          if (!matchedContact && fromAddr && !ownEmailsImap.has(fromAddr) && !isAutoSender2) {
            if (!unknownMap.has(fromAddr)) unknownMap.set(fromAddr, { address: fromAddr, name: fromName, count: 0 });
            unknownMap.get(fromAddr).count++;
          }

          const result = db.insertEmail({ message_id: msgId, from_address: fromText, to_address: toText, subject, body_text: bodyText, received_at: date, matched_contact: matchedContact, matched_organization: matchedOrganization });
          if (result.changes) { synced++; if (matchedContact) matched++; }
        } catch {}
      }

      const rawSenders = [...unknownMap.values()].sort((a, b) => b.count - a.count);

      // AI-analys av avsändare i omgångar — ingen försvinner pga gräns
      let unknownSenders = rawSenders;
      if (rawSenders.length) {
        try {
          // Hämta ämnesrader per avsändare för bättre kontext
          const sendersWithSubjects = rawSenders.map(s => {
            const subjects = db.getAllEmails({ limit: 200 })
              .filter(e => e.from_address?.toLowerCase().includes(s.address))
              .slice(0, 5)
              .map(e => e.subject)
              .filter(Boolean);
            return { ...s, subjects };
          });

          const analysis = await filterEmailSendersBatched(sendersWithSubjects);
          unknownSenders = applyAnalysis(sendersWithSubjects, analysis);
        } catch {
          unknownSenders = rawSenders;
        }
      }

      db.setJonnaKey('email_last_sync', new Date().toISOString());
      db.setJonnaKey('email_unknown_senders', unknownSenders);
      res.json({ synced, matched, unknown_senders: unknownSenders });
    } finally {
      lock.release();
    }
    await client.logout();
  } catch (err) {
    let msg = err.message || 'Okänt fel';
    if (msg.includes('Command failed') || msg.includes('AUTHENTICATIONFAILED') || msg.includes('Invalid credentials')) {
      msg = 'Fel användarnamn eller lösenord. Kontrollera att IMAP är aktiverat i Outlook-inställningarna och att du använder rätt lösenord (eller app-lösenord om du har tvåfaktorsautentisering).';
    } else if (msg.includes('ECONNREFUSED') || msg.includes('ENOTFOUND') || msg.includes('timeout')) {
      msg = 'Kunde inte ansluta till Outlook. Kontrollera din internetanslutning.';
    }
    res.status(500).json({ error: msg });
  }
});

app.get('/api/emails', (req, res) => {
  res.json(db.getAllEmails());
});

// ── Återbygg unknown senders från befintliga mejl ──────────────
app.post('/api/emails/rebuild-senders', async (req, res) => {
  try {
    const contacts = db.getContacts();
    const ownEmail = (db.getJonnaKey('own_email') || db.getJonnaKey('email_settings')?.user || '').toLowerCase();
    const feedback = db.getJonnaKey('email_sender_feedback') || [];
    const rejectedAddresses = new Set(feedback.filter(f => f.decision === 'rejected').map(f => f.address));

    const rows = db.getUnmatchedSenderCounts();

    const unknownMap = new Map();
    for (const row of rows) {
      const raw = row.from_address || '';
      // from_address kan vara "Namn <addr>" — extrahera adressen
      const match = raw.match(/<([^>]+)>/) || [null, raw];
      const addr = (match[1] || raw).toLowerCase().trim();
      const name = raw.includes('<') ? raw.split('<')[0].trim().replace(/^"|"$/g, '') : addr;

      if (!addr || addr === ownEmail) continue;
      const localPart = addr.split('@')[0];
      if (/^(noreply|no-reply|no\.reply|donotreply|do-not-reply)/.test(localPart)) continue;
      if (rejectedAddresses.has(addr)) continue;

      const isContact = contacts.some(c => {
        const extras = (() => { try { return JSON.parse(c.extra_emails || '[]'); } catch { return []; } })();
        return [c.email, ...extras].filter(Boolean).map(e => e.toLowerCase()).includes(addr);
      });
      if (isContact) continue;

      if (!unknownMap.has(addr)) {
        const subjects = db.getSubjectsForSender(addr);
        unknownMap.set(addr, { address: addr, name, count: row.count, subjects });
      }
    }

    const unknownSenders = [...unknownMap.values()].sort((a, b) => b.count - a.count);
    console.log(`[rebuild] ${unknownSenders.length} unika avsändare hittade, kör AI-analys...`);

    const batchSize = 50;
    const totalBatches = Math.ceil(unknownSenders.length / batchSize);
    let allFiltered = [];

    for (let i = 0; i < unknownSenders.length; i += batchSize) {
      const batch = unknownSenders.slice(i, i + batchSize);
      const batchNum = Math.floor(i / batchSize) + 1;
      try {
        const batchAnalysis = await filterEmailSenders(batch);
        const batchFiltered = applyAnalysis(batch, batchAnalysis);
        allFiltered = [...allFiltered, ...batchFiltered];
        db.setJonnaKey('email_unknown_senders', allFiltered); // spara efter varje batch
        console.log(`[AI-filter] Omgång ${batchNum}/${totalBatches} klar — ${batchFiltered.length} relevanta (totalt ${allFiltered.length})`);
      } catch (err) {
        console.error(`[AI-filter] Omgång ${batchNum}/${totalBatches} misslyckades: ${err.message} — hoppar över`);
      }
    }

    console.log(`[rebuild] Klar! ${allFiltered.length} kontaktförslag sparade.`);
    res.json({ count: allFiltered.length });
  } catch (err) {
    console.error('[rebuild] Krasch:', err);
    res.status(500).json({ error: err.message });
  }
});

app.post('/api/emails/:id/match', (req, res) => {
  const { contact_id } = req.body;
  db.matchEmail(req.params.id, contact_id);
  res.json({ ok: true });
});

// ── Applications ───────────────────────────────────────────────

app.get('/api/applications', (req, res) => {
  const { status, limit } = req.query;
  res.json(db.getApplications({ status, limit: limit ? parseInt(limit) : 50 }));
});

app.get('/api/applications/:id', (req, res) => {
  const app_ = db.getApplication(req.params.id);
  if (!app_) return res.status(404).json({ error: 'Inte hittad' });
  res.json(app_);
});

app.patch('/api/applications/:id', (req, res) => {
  db.updateApplication(req.params.id, req.body);
  res.json(db.getApplication(req.params.id));
});

app.delete('/api/applications/:id', (req, res) => {
  db.deleteApplication(req.params.id);
  res.json({ ok: true });
});

app.post('/api/applications/:id/save-as-sample', (req, res) => {
  const app_ = db.getApplication(req.params.id);
  if (!app_) return res.status(404).json({ error: 'Inte hittad' });
  const text = app_.edited_text || app_.generated_text || '';
  if (!text) return res.status(400).json({ error: 'Ingen text att spara' });
  let samples = [];
  try { samples = db.getJonnaKey('application_samples') || []; } catch {}
  if (!Array.isArray(samples)) samples = [];
  samples.push({
    id: Date.now(),
    title: app_.opportunity_title || 'Utan titel',
    text,
    type: app_.document_type,
    added_at: new Date().toISOString()
  });
  db.setJonnaKey('application_samples', samples);
  res.json({ ok: true });
});

app.post('/api/applications/analyze-opportunity', async (req, res) => {
  const { title, organization, description, url, opportunity_type, opportunity_id } = req.body;

  let fullText = description || '';
  if (url) {
    try {
      const ctrl = new AbortController();
      const t = setTimeout(() => ctrl.abort(), 8000);
      const r = await fetch(url, { signal: ctrl.signal, headers: { 'User-Agent': 'Mozilla/5.0' } });
      clearTimeout(t);
      let html = await r.text();
      fullText = html.replace(/<[^>]+>/g, ' ').replace(/\s+/g, ' ').trim().slice(0, 3000);
    } catch {}
    if (!fullText) fullText = description || '';
  }

  const prompt = `Analysera denna utlysning/jobbannons för en skådespelerska och extrahera strukturerad information.

Titel: ${title || ''}
Organisation: ${organization || ''}
Text: ${fullText}

Svara ENBART med JSON:
{
  "role_description": "Vad rollen/tjänsten innebär (2-3 meningar)",
  "requirements": ["krav 1", "krav 2"],
  "what_to_submit": ["CV", "personligt brev"],
  "key_themes": ["tema 1", "tema 2"],
  "deadline": "sista ansökningsdag eller null",
  "contact_person": "kontaktperson eller null",
  "tone": "formal eller informal",
  "language": "svenska eller engelska"
}`;

  let analysis = {};
  try {
    const msg = await anthropic.messages.create({
      model: 'claude-haiku-4-5-20251001',
      max_tokens: 800,
      messages: [{ role: 'user', content: prompt }]
    });
    const text_ = msg.content[0].text;
    const m = text_.match(/\{[\s\S]*\}/);
    if (m) analysis = JSON.parse(m[0]);
  } catch {}

  const result = db.insertApplication({
    opportunity_type: opportunity_type || 'manual',
    opportunity_id: opportunity_id || null,
    opportunity_title: title || '',
    opportunity_organization: organization || '',
    opportunity_url: url || '',
    opportunity_deadline: analysis.deadline || null,
    ai_analysis: JSON.stringify(analysis)
  });

  res.json({ applicationId: result.lastInsertRowid, analysis });
});

app.post('/api/applications/generate', async (req, res) => {
  const { applicationId, documentType, extraInstructions } = req.body;
  if (!applicationId) return res.status(400).json({ error: 'applicationId saknas' });

  const app_ = db.getApplication(applicationId);
  if (!app_) return res.status(404).json({ error: 'Ansökan inte hittad' });

  const jonnaContext = getJonnaFullContext();
  let analysis = {};
  try { analysis = JSON.parse(app_.ai_analysis || '{}'); } catch {}

  const docType = documentType || app_.document_type || 'personal_letter';
  const docLabel = { personal_letter: 'PERSONLIGT BREV', application: 'ANSÖKAN', cover_letter: 'SÖKBREV' }[docType] || 'PERSONLIGT BREV';
  const lengthGuide = { personal_letter: '400–600 ord', application: '500–800 ord', cover_letter: '200–300 ord' }[docType] || '400–600 ord';

  const systemMsg = `Du är en expert på att skriva skådespeleriansökningar för svenska teater- och filmproduktioner. Du skriver i förstaperson från Jonnas perspektiv. Skriv genuint, konkret och personligt — som låter som Jonna, inte som en mall. Svara ENBART med den färdiga texten, inga rubriker, inga förklaringar, inget annat.`;

  const analysisText = `Krav: ${(analysis.requirements || []).join(', ') || 'ej specificerat'}
Ska skickas in: ${(analysis.what_to_submit || []).join(', ') || 'ej specificerat'}
Nyckelord: ${(analysis.key_themes || []).join(', ') || ''}
Rollbeskrivning: ${analysis.role_description || ''}`;

  const userMsg = `${jonnaContext}

---
UTLYSNINGEN:
Titel: ${app_.opportunity_title || ''}
Organisation: ${app_.opportunity_organization || ''}
${analysisText}
${extraInstructions ? `\nExtra instruktioner från Jonna: ${extraInstructions}` : ''}

---
INSTRUKTIONER:
Skriv ett ${docLabel} på svenska.
Längd: ${lengthGuide}.
Regler: Inga klichéer som "passionerad skådespelerska" eller liknande. Börja INTE med "Jag heter". Nämn 2–3 SPECIFIKA produktioner från listan ovan som är relevanta för denna utlysning. Var konkret och personlig.`;

  try {
    const msg = await anthropic.messages.create({
      model: 'claude-sonnet-4-6',
      max_tokens: 1500,
      temperature: 0.7,
      system: systemMsg,
      messages: [{ role: 'user', content: userMsg }]
    });
    const generatedText = msg.content[0].text.trim();
    db.updateApplication(applicationId, { generated_text: generatedText, edited_text: generatedText, document_type: docType, generation_prompt: userMsg.slice(0, 500) });
    res.json({ applicationId, text: generatedText });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ── Discover: Castingkall ──────────────────────────────────────
app.get('/api/discover/castings', (req, res) => {
  res.json(db.getCastings());
});

app.post('/api/discover/castings/search', async (req, res) => {
  const jonnaProfile = db.getJonnaProfile();
  let actorAttrs = null;
  try { actorAttrs = typeof jonnaProfile.actor_attributes === 'string' ? JSON.parse(jonnaProfile.actor_attributes) : jonnaProfile.actor_attributes; } catch {}
  const playingAge = actorAttrs?.playing_age || (actorAttrs?.age_range_min ? `${actorAttrs.age_range_min}–${actorAttrs.age_range_max}` : null);
  const languages = actorAttrs?.languages?.join(', ') || 'svenska';

  const profileHint = [
    playingAge ? `spelålder ${playingAge}` : '',
    languages ? `språk: ${languages}` : '',
    actorAttrs?.voice_type ? `röst: ${actorAttrs.voice_type}` : ''
  ].filter(Boolean).join(', ');
  const searchSummary = getJonnaSearchSummary() + (profileHint ? ` Fysiska attribut: ${profileHint}.` : '');

  try {
    if (req.query.reset === 'true') db.clearCastings();
    const scrapedContent = await scrapeJobSites();
    const scrapedSection = scrapedContent ? `\n\nHär är scrapad data från jobbannonssajter — använd detta som primär källa:\n${scrapedContent}` : '';

    const prompt = `Du ska hitta aktuella castingkall och auditions för skådespelare i Sverige.${scrapedSection}

Komplettera även med websökning mot: dramaten.se, stadsteatern.se, riksteatern.se, göteborgsoperan.se, SF Studios, Filmpool Nord, Film i Väst, fria teatergrupper.

Om sökanden: ${searchSummary}

Extrahera casting calls ur den scrapade texten och komplettera med websökning. Hitta 10-15 castingkall/auditions. Svara ENBART med JSON-array:
[{
  "title": "Rollnamn eller produktionstitel",
  "organization": "Teater/produktionsbolag",
  "description": "Beskrivning av rollen eller produktionen (2-3 meningar)",
  "url": "länk",
  "deadline": "sista ansökningsdag eller null",
  "interesting_score": 4,
  "interesting_reason": "Varför detta är relevant (1 mening)",
  "subtype": "casting/audition/open_call"
}]`;

    const text = await claudeSearch(prompt, 5000);
    let listings = [];
    try {
      const jsonMatch = text.match(/\[[\s\S]*\]/);
      listings = JSON.parse(jsonMatch ? jsonMatch[0] : '[]');
    } catch { listings = []; }

    if (listings.length > 0) db.clearCastings();
    const saved = [];
    for (const l of listings) {
      const result = db.insertJobListing({
        title: l.title || null,
        organization: l.organization || null,
        description: l.description || null,
        url: l.url || null,
        deadline: l.deadline || null,
        interesting_score: l.interesting_score || 3,
        interesting_reason: l.interesting_reason || null,
        known_contacts: '[]'
      });
      if (result.lastInsertRowid) {
        try { db.setJobSubtype(result.lastInsertRowid, l.subtype || 'casting'); } catch {}
      }
      saved.push(l);
    }

    if (saved.length) db.setJonnaKey('last_searched_castings', new Date().toISOString());
    res.json(saved);
  } catch (err) {
    const status = err.status === 529 ? 529 : err.status === 429 ? 429 : 500;
    res.status(status).json({ error: err.message });
  }
});

app.get('/download-db', (req, res) => {
  const dbPath = process.env.DB_PATH || path.join(__dirname, 'crm.db');
  res.download(dbPath, 'crm.db');
});

const PORT = process.env.PORT || 3001;
app.listen(PORT, () => console.log(`Actress CRM: http://localhost:${PORT}`));
