"""Generate docs/presentation.pptx - a tight 11-slide PowerPoint for Iceshelf.

Run from repo root:
    python docs/generate_pptx.py

Produces: docs/presentation.pptx

Design notes (why 11 slides, not 22):
- Product-team feedback on the prior deck: too much content, no integration
  story, no mapping from the producer journey back to skills/agents.
- This revision: narrative flow in under 15 minutes, with two anchor slides:
    (4) How Iceshelf plugs into your producer's repo
    (5) Producer journey with skills/agents called out at each stage
"""
from pathlib import Path
from pptx import Presentation
from pptx.util import Inches, Pt, Emu
from pptx.dml.color import RGBColor
from pptx.enum.shapes import MSO_SHAPE
from pptx.enum.text import PP_ALIGN, MSO_ANCHOR

# ---------- palette ----------
BG_DARK      = RGBColor(0x0B, 0x10, 0x20)
BG_SURFACE   = RGBColor(0x11, 0x18, 0x27)
BG_SURFACE_2 = RGBColor(0x0E, 0x16, 0x27)
BORDER       = RGBColor(0x1F, 0x2A, 0x44)
BORDER_STR   = RGBColor(0x2B, 0x3A, 0x5F)
TEXT         = RGBColor(0xE6, 0xED, 0xF7)
TEXT_DIM     = RGBColor(0x9A, 0xA7, 0xBD)
TEXT_FAINT   = RGBColor(0x6B, 0x78, 0x90)
CYAN         = RGBColor(0x22, 0xD3, 0xEE)
EMERALD      = RGBColor(0x34, 0xD3, 0x99)
VIOLET       = RGBColor(0xA7, 0x8B, 0xFA)
AMBER        = RGBColor(0xFB, 0xBF, 0x24)
ROSE         = RGBColor(0xFB, 0x71, 0x85)

FONT_SANS = "Calibri"
FONT_MONO = "Consolas"

SLIDE_W = Inches(13.333)
SLIDE_H = Inches(7.5)

prs = Presentation()
prs.slide_width  = SLIDE_W
prs.slide_height = SLIDE_H
BLANK = prs.slide_layouts[6]

# ---------- primitives ----------

def set_bg(slide, color=BG_DARK):
    slide.background.fill.solid()
    slide.background.fill.fore_color.rgb = color

def rect(slide, x, y, w, h, fill=None, line=None, line_w=None):
    shp = slide.shapes.add_shape(MSO_SHAPE.RECTANGLE, x, y, w, h)
    shp.shadow.inherit = False
    if fill is None:
        shp.fill.background()
    else:
        shp.fill.solid(); shp.fill.fore_color.rgb = fill
    if line is None:
        shp.line.fill.background()
    else:
        shp.line.color.rgb = line
        if line_w is not None:
            shp.line.width = line_w
    return shp

def rrect(slide, x, y, w, h, *, fill=BG_SURFACE_2, line=BORDER):
    shp = slide.shapes.add_shape(MSO_SHAPE.ROUNDED_RECTANGLE, x, y, w, h)
    shp.shadow.inherit = False
    shp.fill.solid(); shp.fill.fore_color.rgb = fill
    if line is None:
        shp.line.fill.background()
    else:
        shp.line.color.rgb = line
        shp.line.width = Emu(6350)
    return shp

def text(slide, x, y, w, h, s, *, size=14, color=TEXT, bold=False, italic=False,
         font=FONT_SANS, align=PP_ALIGN.LEFT, anchor=MSO_ANCHOR.TOP):
    tb = slide.shapes.add_textbox(x, y, w, h)
    tf = tb.text_frame
    tf.margin_left = tf.margin_right = Emu(0)
    tf.margin_top = tf.margin_bottom = Emu(0)
    tf.word_wrap = True
    tf.vertical_anchor = anchor
    p = tf.paragraphs[0]; p.alignment = align
    r = p.add_run(); r.text = s
    r.font.name = font; r.font.size = Pt(size)
    r.font.bold = bold; r.font.italic = italic
    r.font.color.rgb = color
    return tb

def multi_text(slide, x, y, w, h, lines, *, line_space=1.2,
               anchor=MSO_ANCHOR.TOP, align=PP_ALIGN.LEFT):
    """lines: list of [(text, opts), ...]. opts: size,color,bold,italic,font."""
    tb = slide.shapes.add_textbox(x, y, w, h)
    tf = tb.text_frame
    tf.margin_left = tf.margin_right = Emu(0)
    tf.margin_top = tf.margin_bottom = Emu(0)
    tf.word_wrap = True
    tf.vertical_anchor = anchor
    for i, runs in enumerate(lines):
        p = tf.paragraphs[0] if i == 0 else tf.add_paragraph()
        p.alignment = align; p.line_spacing = line_space
        for s, opts in runs:
            r = p.add_run(); r.text = s
            r.font.name = opts.get("font", FONT_SANS)
            r.font.size = Pt(opts.get("size", 12))
            r.font.bold = opts.get("bold", False)
            r.font.italic = opts.get("italic", False)
            r.font.color.rgb = opts.get("color", TEXT)
    return tb

def bullets(slide, x, y, w, h, items, *, size=12, color=TEXT,
            bullet_color=CYAN, line_space=1.3):
    tb = slide.shapes.add_textbox(x, y, w, h)
    tf = tb.text_frame
    tf.margin_left = tf.margin_right = Emu(0)
    tf.margin_top = tf.margin_bottom = Emu(0)
    tf.word_wrap = True
    for i, item in enumerate(items):
        p = tf.paragraphs[0] if i == 0 else tf.add_paragraph()
        p.line_spacing = line_space; p.space_after = Pt(2)
        r1 = p.add_run(); r1.text = "\u2022  "
        r1.font.name = FONT_SANS; r1.font.size = Pt(size)
        r1.font.color.rgb = bullet_color; r1.font.bold = True
        r2 = p.add_run(); r2.text = item
        r2.font.name = FONT_SANS; r2.font.size = Pt(size)
        r2.font.color.rgb = color
    return tb

def pill(slide, x, y, label, color=CYAN, w=Inches(1.4)):
    h = Inches(0.28)
    shp = slide.shapes.add_shape(MSO_SHAPE.ROUNDED_RECTANGLE, x, y, w, h)
    shp.shadow.inherit = False
    shp.fill.solid(); shp.fill.fore_color.rgb = BG_SURFACE_2
    shp.line.color.rgb = color; shp.line.width = Emu(6350)
    tf = shp.text_frame
    tf.margin_left = Emu(0); tf.margin_right = Emu(0)
    tf.margin_top = Emu(0); tf.margin_bottom = Emu(0)
    tf.vertical_anchor = MSO_ANCHOR.MIDDLE
    p = tf.paragraphs[0]; p.alignment = PP_ALIGN.CENTER
    r = p.add_run(); r.text = label
    r.font.name = FONT_SANS; r.font.size = Pt(9)
    r.font.color.rgb = color; r.font.bold = True
    return shp

def card(slide, x, y, w, h, *, accent=CYAN, fill=BG_SURFACE, left_bar=True):
    c = rect(slide, x, y, w, h, fill=fill, line=BORDER, line_w=Emu(6350))
    if left_bar:
        rect(slide, x, y, Inches(0.06), h, fill=accent, line=None)
    return c

def header(slide, eyebrow, title, *, pills_=None, title_size=30):
    y0 = Inches(0.5)
    if pills_:
        cx = Inches(0.6)
        for label, color in pills_:
            pill(slide, cx, y0, label, color)
            cx += Inches(1.5)
        y0 += Inches(0.38)
    text(slide, Inches(0.6), y0, Inches(12.2), Inches(0.3),
         eyebrow.upper(), size=10, color=CYAN, bold=True)
    text(slide, Inches(0.6), y0 + Inches(0.3), Inches(12.2), Inches(0.9),
         title, size=title_size, color=TEXT, bold=True)
    rect(slide, Inches(0.6), y0 + Inches(1.1), Inches(12.2), Emu(6350),
         fill=BORDER, line=None)

def footer(slide, idx, total):
    text(slide, Inches(0.6), Inches(7.1), Inches(8), Inches(0.3),
         "ICESHELF \u00b7 v1.0.0 \u00b7 Executive briefing",
         size=9, color=TEXT_FAINT, bold=True)
    text(slide, Inches(11.5), Inches(7.1), Inches(1.2), Inches(0.3),
         f"{idx:02d} / {total:02d}", size=9, color=TEXT_FAINT,
         align=PP_ALIGN.RIGHT, bold=True)

def arrow_right(slide, x, y, w, h, color=CYAN):
    a = slide.shapes.add_shape(MSO_SHAPE.RIGHT_ARROW, x, y, w, h)
    a.shadow.inherit = False
    a.fill.solid(); a.fill.fore_color.rgb = color
    a.line.fill.background()
    return a

def add_table(slide, x, y, w, h, headers, rows, *, header_color=CYAN,
              body_size=10, header_size=10):
    t = slide.shapes.add_table(len(rows) + 1, len(headers), x, y, w, h).table
    for j, ht in enumerate(headers):
        c = t.cell(0, j); c.fill.solid(); c.fill.fore_color.rgb = BG_SURFACE_2
        tf = c.text_frame
        tf.margin_left = Inches(0.08); tf.margin_right = Inches(0.08)
        tf.margin_top = Inches(0.04); tf.margin_bottom = Inches(0.04)
        tf.word_wrap = True
        p = tf.paragraphs[0]; p.text = ""
        r = p.add_run(); r.text = ht
        r.font.name = FONT_SANS; r.font.size = Pt(header_size); r.font.bold = True
        r.font.color.rgb = header_color
    for i, row in enumerate(rows, start=1):
        for j, v in enumerate(row):
            c = t.cell(i, j); c.fill.solid()
            c.fill.fore_color.rgb = BG_SURFACE if i % 2 else BG_SURFACE_2
            tf = c.text_frame
            tf.margin_left = Inches(0.08); tf.margin_right = Inches(0.08)
            tf.margin_top = Inches(0.04); tf.margin_bottom = Inches(0.04)
            tf.word_wrap = True
            p = tf.paragraphs[0]; p.text = ""
            if isinstance(v, tuple):
                s, col, bold = v
            else:
                s, col, bold = v, TEXT, False
            r = p.add_run(); r.text = s
            r.font.name = FONT_SANS; r.font.size = Pt(body_size)
            r.font.bold = bold; r.font.color.rgb = col
    return t

# ---------- slides ----------

def s1_title():
    s = prs.slides.add_slide(BLANK); set_bg(s)
    text(s, Inches(0.6), Inches(0.5), Inches(6), Inches(0.3),
         "ICESHELF \u00b7 v1.0.0", size=11, color=CYAN, bold=True)
    text(s, Inches(10.5), Inches(0.5), Inches(2.3), Inches(0.3),
         "Apache-2.0", size=11, color=TEXT_FAINT, bold=True, align=PP_ALIGN.RIGHT)

    text(s, Inches(0.6), Inches(1.6), Inches(12), Inches(0.4),
         "APACHE ICEBERG FOR AWS DATA MESH", size=12, color=VIOLET, bold=True)
    text(s, Inches(0.6), Inches(2.2), Inches(12), Inches(1.6),
         "Onboard producers to Iceberg \u2014\ncode generated into your repo.",
         size=42, color=TEXT, bold=True)
    text(s, Inches(0.6), Inches(4.2), Inches(12), Inches(0.6),
         "A Claude Code plugin \u2014 skills and subagents that write production-ready",
         size=16, color=TEXT_DIM)
    text(s, Inches(0.6), Inches(4.55), Inches(12), Inches(0.6),
         "Iceberg code, IaC, and runbooks into your producer's existing repository.",
         size=16, color=TEXT_DIM)

    # three-word differentiators
    y = Inches(5.4); cx = Inches(0.6)
    for label, color in [
        ("Your repo. Your code.", CYAN),
        ("No SaaS runtime.", EMERALD),
        ("Multi-region by default.", VIOLET),
        ("Apache-2.0.", AMBER),
    ]:
        pill(s, cx, y, label, color, w=Inches(3.0))
        cx += Inches(3.1)

    rect(s, Inches(0.6), Inches(6.3), Inches(12.2), Emu(6350), fill=BORDER, line=None)
    text(s, Inches(0.6), Inches(6.45), Inches(8), Inches(0.4),
         "For senior management and senior architects.",
         size=13, color=TEXT_DIM, italic=True)

def s2_problem():
    s = prs.slides.add_slide(BLANK); set_bg(s)
    header(s, "Problem",
           "Every producer reinvents Iceberg. Multi-region makes it worse.",
           pills_=[("Leadership", AMBER), ("Architect", VIOLET)])
    y = Inches(2.3); w = Inches(4.0); h = Inches(3.8); gap = Inches(0.2)
    cards = [
        ("Six stacks, six sets of quirks", CYAN, [
            "Glue / EMR PySpark, ECS / Lambda Python, ECS / Lambda Java",
            "Each re-learns the same Iceberg configuration traps",
            "No shared code, no shared review checklist",
        ]),
        ("Multi-region is hard under strict rules", VIOLET, [
            "No cross-region S3. No MRAPs.",
            "Metadata repointing is non-trivial and error-prone",
            "Reference material assumes the opposite posture",
        ]),
        ("No platform leverage", EMERALD, [
            "Compaction, snapshot expiry, orphan cleanup diverge per team",
            "Security review reopens from zero on every onboarding",
            "Policy cannot be enforced in one place",
        ]),
    ]
    x = Inches(0.6)
    for title, color, items in cards:
        card(s, x, y, w, h, accent=color)
        text(s, x + Inches(0.3), y + Inches(0.3), w - Inches(0.6), Inches(0.5),
             title, size=16, color=color, bold=True)
        bullets(s, x + Inches(0.3), y + Inches(1.1), w - Inches(0.6), h - Inches(1.3),
                items, size=12, bullet_color=color, line_space=1.35)
        x += w + gap
    text(s, Inches(0.6), Inches(6.4), Inches(12), Inches(0.4),
         "Result: 10\u201312 person-weeks per producer, repeated across the mesh.",
         size=14, color=AMBER, italic=True, bold=True)

def s3_what_it_is():
    s = prs.slides.add_slide(BLANK); set_bg(s)
    header(s, "What Iceshelf is",
           "A Claude Code plugin that writes Iceberg code into your repo.",
           pills_=[("All audiences", CYAN)])
    # three-part definition
    y = Inches(2.3); w = Inches(4.0); h = Inches(3.0); gap = Inches(0.2)
    parts = [
        ("9 skills", CYAN, "Slash commands that drive each stage of onboarding \u2014 from profile capture through multi-region DR."),
        ("5 subagents", VIOLET, "Specialized reasoning: architect, code-generator, validator, multi-region planner, orchestrator."),
        ("Presets + profiles", EMERALD, "Org-wide policy as YAML. Each producer inherits it; only overrides go in the profile."),
    ]
    x = Inches(0.6)
    for title, color, body in parts:
        card(s, x, y, w, h, accent=color)
        text(s, x + Inches(0.3), y + Inches(0.3), w - Inches(0.6), Inches(0.7),
             title, size=22, color=color, bold=True)
        text(s, x + Inches(0.3), y + Inches(1.2), w - Inches(0.6), h - Inches(1.4),
             body, size=13, color=TEXT_DIM)
        x += w + gap
    # bottom call-out: what you get back
    y2 = Inches(5.6); h2 = Inches(1.2)
    card(s, Inches(0.6), y2, Inches(12.2), h2, accent=AMBER, left_bar=True)
    text(s, Inches(0.85), y2 + Inches(0.2), Inches(11.5), Inches(0.4),
         "What ships back to you", size=14, color=AMBER, bold=True)
    multi_text(s, Inches(0.85), y2 + Inches(0.6), Inches(11.5), Inches(0.6), [
        [("PySpark / PyIceberg / Java pipeline code", {"size":12,"color":TEXT,"bold":True}),
         ("  \u00b7  Terraform for S3 / Glue / IAM / KMS / VPC  \u00b7  ", {"size":12,"color":TEXT_DIM}),
         ("CloudWatch alarms + dashboards", {"size":12,"color":TEXT,"bold":True}),
         ("  \u00b7  ", {"size":12,"color":TEXT_DIM}),
         ("DR runbook", {"size":12,"color":TEXT,"bold":True}),
         ("  \u2014  all committed to your repo, reviewable in a normal PR.", {"size":12,"color":TEXT_DIM})]
    ], line_space=1.1)

def s4_integration():
    """How Iceshelf plugs into the producer repo."""
    s = prs.slides.add_slide(BLANK); set_bg(s)
    header(s, "Integration \u00b7 how it plugs into your repo",
           "Claude Code runs locally. Files land in your repo. Your CI/CD deploys.",
           pills_=[("Leadership", AMBER), ("Architect", VIOLET), ("Developer", EMERALD)])

    # LEFT: repo tree
    lx = Inches(0.6); ly = Inches(2.2); lw = Inches(5.2); lh = Inches(4.6)
    card(s, lx, ly, lw, lh, accent=EMERALD, left_bar=False)
    rect(s, lx, ly, lw, Inches(0.08), fill=EMERALD, line=None)
    text(s, lx + Inches(0.25), ly + Inches(0.18), lw - Inches(0.5), Inches(0.4),
         "producer-repo/", size=12, color=EMERALD, bold=True, font=FONT_MONO)
    tree_lines = [
        [("\u251c\u2500 ", {"size":11,"color":TEXT_FAINT,"font":FONT_MONO}),
         ("src/                     ", {"size":11,"color":TEXT_DIM,"font":FONT_MONO}),
         (" # your existing code", {"size":10,"color":TEXT_FAINT,"font":FONT_MONO,"italic":True})],
        [("\u251c\u2500 ", {"size":11,"color":TEXT_FAINT,"font":FONT_MONO}),
         ("tests/                   ", {"size":11,"color":TEXT_DIM,"font":FONT_MONO}),
         (" # your existing tests", {"size":10,"color":TEXT_FAINT,"font":FONT_MONO,"italic":True})],
        [("\u251c\u2500 ", {"size":11,"color":TEXT_FAINT,"font":FONT_MONO}),
         (".github/workflows/       ", {"size":11,"color":TEXT_DIM,"font":FONT_MONO}),
         (" # your existing CI/CD", {"size":10,"color":TEXT_FAINT,"font":FONT_MONO,"italic":True})],
        [("\u2502", {"size":11,"color":TEXT_FAINT,"font":FONT_MONO})],
        [("\u251c\u2500 ", {"size":11,"color":CYAN,"font":FONT_MONO,"bold":True}),
         ("profiles/<producer>.yaml ", {"size":11,"color":CYAN,"font":FONT_MONO,"bold":True}),
         (" # Iceshelf: org preset + overrides", {"size":10,"color":CYAN,"font":FONT_MONO,"italic":True})],
        [("\u251c\u2500 ", {"size":11,"color":CYAN,"font":FONT_MONO,"bold":True}),
         ("iceberg/", {"size":11,"color":CYAN,"font":FONT_MONO,"bold":True})],
        [("\u2502   \u251c\u2500 ", {"size":11,"color":CYAN,"font":FONT_MONO}),
         ("ddl/*.sql            ", {"size":11,"color":CYAN,"font":FONT_MONO,"bold":True}),
         (" # Iceshelf: table DDL", {"size":10,"color":CYAN,"font":FONT_MONO,"italic":True})],
        [("\u2502   \u251c\u2500 ", {"size":11,"color":CYAN,"font":FONT_MONO}),
         ("pipeline/*.py        ", {"size":11,"color":CYAN,"font":FONT_MONO,"bold":True}),
         (" # Iceshelf: ingest + upsert jobs", {"size":10,"color":CYAN,"font":FONT_MONO,"italic":True})],
        [("\u2502   \u2514\u2500 ", {"size":11,"color":CYAN,"font":FONT_MONO}),
         ("maintenance/*.py     ", {"size":11,"color":CYAN,"font":FONT_MONO,"bold":True}),
         (" # Iceshelf: compaction, expiry", {"size":10,"color":CYAN,"font":FONT_MONO,"italic":True})],
        [("\u251c\u2500 ", {"size":11,"color":CYAN,"font":FONT_MONO,"bold":True}),
         ("infra/iceberg/*.tf       ", {"size":11,"color":CYAN,"font":FONT_MONO,"bold":True}),
         (" # Iceshelf: S3/Glue/IAM/KMS", {"size":10,"color":CYAN,"font":FONT_MONO,"italic":True})],
        [("\u2514\u2500 ", {"size":11,"color":CYAN,"font":FONT_MONO,"bold":True}),
         ("runbooks/failover.md     ", {"size":11,"color":CYAN,"font":FONT_MONO,"bold":True}),
         (" # Iceshelf: DR runbook", {"size":10,"color":CYAN,"font":FONT_MONO,"italic":True})],
    ]
    multi_text(s, lx + Inches(0.3), ly + Inches(0.6), lw - Inches(0.6), lh - Inches(0.8),
               tree_lines, line_space=1.15)
    text(s, lx + Inches(0.3), ly + lh - Inches(0.4), lw - Inches(0.6), Inches(0.3),
         "Cyan = new files added by Iceshelf", size=10, color=CYAN, italic=True)

    # RIGHT: flow steps
    rx = lx + lw + Inches(0.2); rw = Inches(6.8); ry = Inches(2.2); rh = Inches(4.6)
    card(s, rx, ry, rw, rh, accent=VIOLET)
    text(s, rx + Inches(0.3), ry + Inches(0.25), rw - Inches(0.5), Inches(0.4),
         "How it flows", size=14, color=VIOLET, bold=True)
    steps = [
        ("1", "Install the Iceshelf plugin in your Claude Code CLI.",
              "Nothing leaves your machine. No account to sign up for."),
        ("2", "Run /iceberg-profile-setup in the producer repo.",
              "Captures region, bucket, stack, DR posture \u2192 profiles/<producer>.yaml."),
        ("3", "Run /iceberg-onboard \u2014 orchestrator drives the rest.",
              "Generates DDL, pipeline code, IaC, runbooks into the tree shown on the left."),
        ("4", "Review the PR. You own every line.",
              "Your existing CI/CD deploys exactly as it does for any other code change."),
        ("5", "Ongoing: slash commands for day-2 operations.",
              "Schema evolution, multi-region, maintenance \u2014 same pattern, same repo."),
    ]
    sy = ry + Inches(0.8)
    for n, title, body in steps:
        # circled number
        c = s.shapes.add_shape(MSO_SHAPE.OVAL, rx + Inches(0.3), sy,
                                Inches(0.38), Inches(0.38))
        c.shadow.inherit = False
        c.fill.solid(); c.fill.fore_color.rgb = VIOLET
        c.line.fill.background()
        tfn = c.text_frame
        tfn.margin_left = Emu(0); tfn.margin_right = Emu(0)
        tfn.margin_top = Emu(0); tfn.margin_bottom = Emu(0)
        tfn.vertical_anchor = MSO_ANCHOR.MIDDLE
        pn = tfn.paragraphs[0]; pn.alignment = PP_ALIGN.CENTER
        rn = pn.add_run(); rn.text = n
        rn.font.name = FONT_SANS; rn.font.size = Pt(12); rn.font.bold = True
        rn.font.color.rgb = BG_DARK
        # title + body
        text(s, rx + Inches(0.8), sy - Inches(0.02), rw - Inches(1.0), Inches(0.35),
             title, size=12, color=TEXT, bold=True)
        text(s, rx + Inches(0.8), sy + Inches(0.3), rw - Inches(1.0), Inches(0.35),
             body, size=10, color=TEXT_DIM)
        sy += Inches(0.72)

    # promise strip
    py = Inches(6.85)
    text(s, Inches(0.6), py, Inches(12.2), Inches(0.35),
         "Iceshelf never runs at your runtime. Iceshelf never sees your data. Iceshelf has no cloud account of its own.",
         size=11, color=TEXT_DIM, italic=True, align=PP_ALIGN.CENTER)

def s5_journey():
    """Producer journey with skills/agents mapped to each phase."""
    s = prs.slides.add_slide(BLANK); set_bg(s)
    header(s, "Producer journey \u00b7 skills & agents at each stage",
           "Five phases. Each with a skill you run and an agent that drives it.",
           pills_=[("Architect", VIOLET), ("Developer", EMERALD)])

    y = Inches(2.25); h = Inches(4.5)
    phases = [
        ("01", "Profile", CYAN,
         "profiles/<producer>.yaml",
         "/iceberg-profile-setup",
         "iceberg-architect\n(validates)"),
        ("02", "Assess", VIOLET,
         "assessment report\n(existing state \u2192 target)",
         "/iceberg-onboard",
         "iceberg-orchestrator\n\u2192 iceberg-architect"),
        ("03", "Generate", EMERALD,
         "iceberg/ddl, pipeline/,\nmaintenance/,\ninfra/iceberg/*.tf",
         "/iceberg-ddl\n/iceberg-pipeline\n/iceberg-data\n/iceberg-maintenance",
         "iceberg-code-\ngenerator"),
        ("04", "Multi-region", AMBER,
         "repoint utility\nDR runbook\nCRR Terraform",
         "/iceberg-multi-region",
         "iceberg-multi-\nregion-planner"),
        ("05", "Promote", CYAN,
         "diagnostics clean\npilot-to-prod\nsign-off",
         "/iceberg-info\n/iceberg-migrate*",
         "iceberg-validator"),
    ]
    w = Inches(2.45); gap = Inches(0.08)
    x = Inches(0.6)
    for i, (num, title, color, artifact, skill, agent) in enumerate(phases):
        card(s, x, y, w, h, accent=color)
        # phase number + title
        text(s, x + Inches(0.2), y + Inches(0.2), w - Inches(0.4), Inches(0.5),
             num, size=24, color=color, bold=True)
        text(s, x + Inches(0.2), y + Inches(0.75), w - Inches(0.4), Inches(0.4),
             title, size=14, color=TEXT, bold=True)
        # divider
        rect(s, x + Inches(0.2), y + Inches(1.25), w - Inches(0.4), Emu(6350),
             fill=BORDER, line=None)
        # artifact
        text(s, x + Inches(0.2), y + Inches(1.4), w - Inches(0.4), Inches(0.25),
             "OUTPUT", size=8, color=TEXT_FAINT, bold=True)
        text(s, x + Inches(0.2), y + Inches(1.65), w - Inches(0.4), Inches(0.9),
             artifact, size=10, color=TEXT_DIM, font=FONT_MONO)
        # skill
        text(s, x + Inches(0.2), y + Inches(2.65), w - Inches(0.4), Inches(0.25),
             "SKILL", size=8, color=TEXT_FAINT, bold=True)
        text(s, x + Inches(0.2), y + Inches(2.9), w - Inches(0.4), Inches(1.0),
             skill, size=10, color=color, font=FONT_MONO, bold=True)
        # agent
        text(s, x + Inches(0.2), y + Inches(3.85), w - Inches(0.4), Inches(0.25),
             "AGENT", size=8, color=TEXT_FAINT, bold=True)
        text(s, x + Inches(0.2), y + Inches(4.1), w - Inches(0.4), Inches(0.6),
             agent, size=10, color=TEXT, italic=True)
        # arrow between phases
        if i < len(phases) - 1:
            ax = x + w + Emu(5000)
            ay = y + h/2
            arrow_right(s, ax, ay - Inches(0.12), gap - Emu(10000), Inches(0.24),
                        color=BORDER_STR)
        x += w + gap

    text(s, Inches(0.6), Inches(6.85), Inches(12.2), Inches(0.3),
         "* /iceberg-migrate is used when the producer is moving an existing Parquet table \u2014 otherwise skip.",
         size=10, color=TEXT_FAINT, italic=True)

def s6_multi_region():
    s = prs.slides.add_slide(BLANK); set_bg(s)
    header(s, "Multi-region \u00b7 the differentiator",
           "DR resilience without cross-region S3 or MRAPs.",
           pills_=[("Architect", VIOLET), ("Leadership", AMBER)])

    # diagram region
    y = Inches(2.2); h = Inches(3.2); pw = Inches(5.3)
    # primary
    card(s, Inches(0.6), y, pw, h, accent=CYAN)
    text(s, Inches(0.85), y + Inches(0.2), pw - Inches(0.5), Inches(0.4),
         "PRIMARY  \u00b7  us-east-1", size=12, color=CYAN, bold=True)
    bullets(s, Inches(0.85), y + Inches(0.75), pw - Inches(0.5), h - Inches(1.0), [
        "S3 warehouse bucket \u2014 data + metadata written locally",
        "Glue catalog with primary metadata_location",
        "Ingest job: glue.region + s3.region pinned locally",
    ], size=12, bullet_color=CYAN, line_space=1.4)

    # DR
    card(s, Inches(7.4), y, pw, h, accent=VIOLET)
    text(s, Inches(7.65), y + Inches(0.2), pw - Inches(0.5), Inches(0.4),
         "DR  \u00b7  us-west-2", size=12, color=VIOLET, bold=True)
    bullets(s, Inches(7.65), y + Inches(0.75), pw - Inches(0.5), h - Inches(1.0), [
        "Replicated bucket via S3 Cross-Region Replication",
        "Repointing utility rewrites metadata + manifests",
        "Local Glue catalog \u2014 queryable in-region",
        "Zero cross-region I/O at runtime",
    ], size=12, bullet_color=VIOLET, line_space=1.4)

    # CRR arrow in middle
    arrow_right(s, Inches(6.0), y + Inches(1.2), Inches(1.3), Inches(0.4), color=EMERALD)
    text(s, Inches(6.0), y + Inches(0.85), Inches(1.3), Inches(0.3),
         "S3 CRR", size=10, color=EMERALD, bold=True, align=PP_ALIGN.CENTER)
    arrow_right(s, Inches(7.3), y + Inches(2.2), Inches(-1.3), Inches(0.4),
                color=AMBER)  # draw arrow then flip via rotation? simpler: draw a label
    text(s, Inches(6.0), y + Inches(1.7), Inches(1.3), Inches(0.3),
         "repoint \u2192", size=10, color=AMBER, bold=True, align=PP_ALIGN.CENTER)

    # RPO/RTO + guarantees
    y2 = Inches(5.6); h2 = Inches(1.4)
    w2 = Inches(6.1); gap = Inches(0.1)
    # LEFT: RPO/RTO
    card(s, Inches(0.6), y2, w2, h2, accent=EMERALD)
    text(s, Inches(0.85), y2 + Inches(0.2), w2 - Inches(0.5), Inches(0.3),
         "Recovery targets (default profile)", size=12, color=EMERALD, bold=True)
    multi_text(s, Inches(0.85), y2 + Inches(0.55), w2 - Inches(0.5), Inches(0.8), [
        [("RPO ", {"size":11,"color":TEXT_DIM,"bold":True}),
         ("\u2264 20 min", {"size":11,"color":EMERALD,"bold":True,"font":FONT_MONO}),
         ("   \u00b7   ", {"size":11,"color":TEXT_FAINT}),
         ("RTO ", {"size":11,"color":TEXT_DIM,"bold":True}),
         ("\u2264 60 min", {"size":11,"color":EMERALD,"bold":True,"font":FONT_MONO}),
         ("   \u00b7   ", {"size":11,"color":TEXT_FAINT}),
         ("repoint every 15 min", {"size":11,"color":TEXT,"font":FONT_MONO})],
        [("Tunable per profile. CloudWatch alarms generated to prove the numbers.",
          {"size":10,"color":TEXT_FAINT,"italic":True})]
    ], line_space=1.3)
    # RIGHT: guarantees
    card(s, Inches(6.8), y2, Inches(6.0), h2, accent=CYAN)
    text(s, Inches(7.05), y2 + Inches(0.2), Inches(5.5), Inches(0.3),
         "Guarantees", size=12, color=CYAN, bold=True)
    multi_text(s, Inches(7.05), y2 + Inches(0.55), Inches(5.5), Inches(0.8), [
        [("\u2713 ", {"size":11,"color":EMERALD,"bold":True}),
         ("Zero cross-region I/O   ", {"size":11,"color":TEXT}),
         ("\u2713 ", {"size":11,"color":EMERALD,"bold":True}),
         ("No MRAPs   ", {"size":11,"color":TEXT}),
         ("\u2713 ", {"size":11,"color":EMERALD,"bold":True}),
         ("Per-region catalog", {"size":11,"color":TEXT})],
        [("\u2713 ", {"size":11,"color":EMERALD,"bold":True}),
         ("Atomic repointing   ", {"size":11,"color":TEXT}),
         ("\u2713 ", {"size":11,"color":EMERALD,"bold":True}),
         ("Auditable runbook   ", {"size":11,"color":TEXT}),
         ("\u2713 ", {"size":11,"color":EMERALD,"bold":True}),
         ("Game-day tested", {"size":11,"color":TEXT})]
    ], line_space=1.4)

def s7_skills_agents():
    s = prs.slides.add_slide(BLANK); set_bg(s)
    header(s, "Skills & subagents \u00b7 cheat sheet",
           "What's in the box.",
           pills_=[("Developer", EMERALD), ("Architect", VIOLET)])
    # LEFT: skills table
    rows = [
        [("/iceberg-profile-setup", CYAN, True),  "Capture producer conventions"],
        [("/iceberg-onboard",        CYAN, True),  "End-to-end orchestrator entry"],
        [("/iceberg-ddl",            CYAN, True),  "Tables + schema evolution"],
        [("/iceberg-data",           CYAN, True),  "Insert / upsert / delete"],
        [("/iceberg-maintenance",    CYAN, True),  "Compaction, expiry, cleanup"],
        [("/iceberg-info",           CYAN, True),  "Diagnostics + metadata"],
        [("/iceberg-migrate",        CYAN, True),  "Parquet / Hive \u2192 Iceberg"],
        [("/iceberg-multi-region",   CYAN, True),  "CRR + repoint + DR catalog"],
        [("/iceberg-pipeline",       CYAN, True),  "End-to-end pipeline gen"],
    ]
    text(s, Inches(0.6), Inches(2.2), Inches(7.5), Inches(0.4),
         "Skills \u2014 slash commands you run", size=13, color=CYAN, bold=True)
    add_table(s, Inches(0.6), Inches(2.6), Inches(7.5), Inches(4.2),
              ["Skill", "Purpose"], rows, header_color=CYAN, body_size=11)

    # RIGHT: subagents
    sub_rows = [
        [("orchestrator",           VIOLET, True), "Coordinates end-to-end"],
        [("architect",               VIOLET, True), "Design + assessment"],
        [("code-generator",          VIOLET, True), "Stack-specific code"],
        [("validator",               VIOLET, True), "Checks before deploy"],
        [("multi-region-planner",    VIOLET, True), "DR + repoint strategy"],
    ]
    text(s, Inches(8.4), Inches(2.2), Inches(4.4), Inches(0.4),
         "Subagents \u2014 specialist reasoning", size=13, color=VIOLET, bold=True)
    add_table(s, Inches(8.4), Inches(2.6), Inches(4.4), Inches(2.4),
              ["Name", "Role"], sub_rows, header_color=VIOLET, body_size=11)

    # tip card
    card(s, Inches(8.4), Inches(5.2), Inches(4.4), Inches(1.6), accent=EMERALD)
    text(s, Inches(8.65), Inches(5.35), Inches(4.0), Inches(0.4),
         "All share one profile", size=12, color=EMERALD, bold=True)
    text(s, Inches(8.65), Inches(5.7), Inches(4.0), Inches(1.0),
         "Every skill and subagent reads profiles/<producer>.yaml. "
         "Producers answer questions once, not per-skill.",
         size=11, color=TEXT_DIM)

def s8_security():
    s = prs.slides.add_slide(BLANK); set_bg(s)
    header(s, "Security & compliance",
           "Defensible by default \u2014 built for security review.",
           pills_=[("Architect", VIOLET), ("Leadership", AMBER)])
    y = Inches(2.2); w = Inches(3.0); h = Inches(3.6); gap = Inches(0.12)
    pillars = [
        ("Identity", CYAN, [
            "IAM scoped to DB + table ARNs",
            "Never \"*\" in generated policies",
            "Read / write / maint roles split",
            "Lake Formation supported",
        ]),
        ("Encryption", VIOLET, [
            "S3 = aws:kms by default",
            "CMK alias pinned in profile",
            "Per-producer key isolation",
            "Key-grant list scripted",
        ]),
        ("Network", EMERALD, [
            "VPC endpoints: S3 / Glue / STS",
            "KMS + CloudWatch private",
            "No public egress required",
            "Private-subnet-only supported",
        ]),
        ("Audit", AMBER, [
            "Profile + code diff-reviewable",
            "Required tags enforced",
            "Optional WORM audit bucket",
            "Retention windows explicit",
        ]),
    ]
    x = Inches(0.6)
    for title, color, items in pillars:
        card(s, x, y, w, h, accent=color)
        text(s, x + Inches(0.25), y + Inches(0.25), w - Inches(0.5), Inches(0.4),
             title, size=14, color=color, bold=True)
        bullets(s, x + Inches(0.25), y + Inches(0.8), w - Inches(0.5), h - Inches(0.95),
                items, size=11, bullet_color=color, line_space=1.35)
        x += w + gap

    # framework strip
    fy = Inches(6.1)
    card(s, Inches(0.6), fy, Inches(12.2), Inches(0.95), accent=CYAN, left_bar=False)
    rect(s, Inches(0.6), fy, Inches(12.2), Inches(0.06), fill=CYAN, line=None)
    text(s, Inches(0.85), fy + Inches(0.2), Inches(5), Inches(0.35),
         "Framework alignment", size=12, color=CYAN, bold=True)
    multi_text(s, Inches(0.85), fy + Inches(0.55), Inches(12), Inches(0.4), [
        [("SOC 2", {"size":11,"color":CYAN,"bold":True}),
         ("   \u00b7   ", {"size":11,"color":TEXT_FAINT}),
         ("HIPAA / PHI", {"size":11,"color":VIOLET,"bold":True}),
         ("   \u00b7   ", {"size":11,"color":TEXT_FAINT}),
         ("PCI-DSS", {"size":11,"color":EMERALD,"bold":True}),
         ("   \u00b7   ", {"size":11,"color":TEXT_FAINT}),
         ("FedRAMP / GovCloud", {"size":11,"color":AMBER,"bold":True}),
         ("   \u2014  aligned by design; artifacts portable to GovCloud with a profile swap.",
          {"size":11,"color":TEXT_DIM,"italic":True})]
    ], line_space=1.1)

def s9_proof():
    s = prs.slides.add_slide(BLANK); set_bg(s)
    header(s, "What ships in v1.0",
           "Six stacks. One quality bar.",
           pills_=[("Leadership", AMBER), ("Architect", VIOLET)])
    # matrix: rows = capability, cols = stacks
    headers = ["Capability", "Glue PySpark", "EMR PySpark", "ECS Py", "Lambda Py", "ECS Java", "Lambda Java"]
    check = ("\u2713", EMERALD, True)
    rows = [
        ["DDL + schema evolution",   check, check, check, check, check, check],
        ["Insert / upsert / delete", check, check, check, check, check, check],
        ["Compaction + expiry",      check, check, check, check, check, check],
        ["Multi-region + repoint",   check, check, check, check, check, check],
        ["IaC (Terraform)",          check, check, check, check, check, check],
        ["DR runbook generated",     check, check, check, check, check, check],
    ]
    add_table(s, Inches(0.6), Inches(2.2), Inches(12.2), Inches(3.6),
              headers, rows, header_color=CYAN, body_size=11, header_size=11)

    # three proof cards
    y = Inches(6.0); w = Inches(4.0); gap = Inches(0.1); x = Inches(0.6)
    proofs = [
        ("100% code is yours", CYAN,
         "No runtime service. Stop using Iceshelf \u2014 keep every file."),
        ("Apache-2.0", EMERALD,
         "No licence gate. Fork it, extend it, bring it in-house."),
        ("No SaaS ingress / egress", AMBER,
         "Nothing from Iceshelf sits on your data path."),
    ]
    for title, color, body in proofs:
        card(s, x, y, w, Inches(1.1), accent=color)
        text(s, x + Inches(0.25), y + Inches(0.12), w - Inches(0.5), Inches(0.35),
             title, size=12, color=color, bold=True)
        text(s, x + Inches(0.25), y + Inches(0.48), w - Inches(0.5), Inches(0.6),
             body, size=10, color=TEXT_DIM)
        x += w + gap

def s10_pilot():
    s = prs.slides.add_slide(BLANK); set_bg(s)
    header(s, "The ask \u00b7 90-day pilot",
           "Two producers. One quarter. Clear decision gate at week 12.",
           pills_=[("Leadership", AMBER)])
    # scope + timeline
    y = Inches(2.2); h = Inches(2.4)
    card(s, Inches(0.6), y, Inches(6.1), h, accent=CYAN)
    text(s, Inches(0.85), y + Inches(0.2), Inches(5.6), Inches(0.35),
         "Scope", size=14, color=CYAN, bold=True)
    bullets(s, Inches(0.85), y + Inches(0.7), Inches(5.6), h - Inches(0.9), [
        "Two producers \u2014 one PySpark, one PyIceberg",
        "One region pair (e.g. us-east-1 / us-west-2)",
        "One preset (aws-glue-datamesh-strict by default)",
        "Dry-run first \u2014 PR-reviewed in sandbox before prod",
    ], size=11, bullet_color=CYAN, line_space=1.35)

    card(s, Inches(6.9), y, Inches(5.9), h, accent=VIOLET)
    text(s, Inches(7.15), y + Inches(0.2), Inches(5.4), Inches(0.35),
         "Timeline", size=14, color=VIOLET, bold=True)
    tl = [
        ["Wk 1\u20132",  "Preset sign-off; sandbox; profiles"],
        ["Wk 3\u20137",  "Producer #1: onboard \u2192 ingest \u2192 multi-region"],
        ["Wk 8\u20139",  "Producer #2 on same preset (reuse proof)"],
        ["Wk 10",         "Game-day: simulated primary-region outage"],
        ["Wk 11\u201312", "Security / cost review \u2192 go/no-go gate"],
    ]
    add_table(s, Inches(7.15), Inches(2.65), Inches(5.4), Inches(1.95),
              ["When", "What"], tl, header_color=VIOLET, body_size=10)

    # exit criteria + ask
    y2 = Inches(4.75); h2 = Inches(2.2)
    card(s, Inches(0.6), y2, Inches(6.1), h2, accent=EMERALD)
    text(s, Inches(0.85), y2 + Inches(0.2), Inches(5.6), Inches(0.35),
         "Exit criteria (all must pass)", size=14, color=EMERALD, bold=True)
    bullets(s, Inches(0.85), y2 + Inches(0.7), Inches(5.6), h2 - Inches(0.9), [
        "Both producers in production, zero hand-edits",
        "\u2264 2 person-weeks / producer (vs 10\u201312 DIY)",
        "RPO \u2264 20 min and RTO \u2264 60 min on game-day",
        "Security + cost review signed off",
    ], size=11, bullet_color=EMERALD, line_space=1.35)

    card(s, Inches(6.9), y2, Inches(5.9), h2, accent=AMBER)
    text(s, Inches(7.15), y2 + Inches(0.2), Inches(5.4), Inches(0.35),
         "What we need from you", size=14, color=AMBER, bold=True)
    bullets(s, Inches(7.15), y2 + Inches(0.7), Inches(5.4), h2 - Inches(0.9), [
        "Executive sponsor (clears org-level blockers)",
        "Platform-team lead: 0.5 FTE \u00d7 12 weeks",
        "Two producer teams: ~0.25 FTE each",
        "Sandbox AWS account with target region pair",
    ], size=11, bullet_color=AMBER, line_space=1.35)

def s11_next():
    s = prs.slides.add_slide(BLANK); set_bg(s)
    header(s, "Next steps",
           "One action for each audience.",
           pills_=[("All audiences", CYAN)])
    y = Inches(2.3); w = Inches(4.0); h = Inches(3.8); gap = Inches(0.2)
    cards = [
        ("Leadership", AMBER,
         ["Approve Iceberg as mesh-wide table format",
          "Sponsor the 2-producer pilot next quarter",
          "Designate platform owner for preset policy"]),
        ("Senior Architects", VIOLET,
         ["Review docs/admin-guide.md",
          "Pick the preset (strict or central-lake)",
          "Define wave-rollout policy"]),
        ("Producer developers", EMERALD,
         ["Read the recipe for your stack",
          "Run /iceberg-profile-setup in your repo",
          "Run /iceberg-onboard and review the PR"]),
    ]
    x = Inches(0.6)
    for title, color, items in cards:
        card(s, x, y, w, h, accent=color)
        pill(s, x + Inches(0.3), y + Inches(0.3), title, color, w=Inches(2.0))
        text(s, x + Inches(0.3), y + Inches(0.8), w - Inches(0.6), Inches(0.4),
             "What to do next", size=14, color=TEXT, bold=True)
        bullets(s, x + Inches(0.3), y + Inches(1.3), w - Inches(0.6), h - Inches(1.5),
                items, size=13, bullet_color=color, line_space=1.4)
        x += w + gap
    # footer
    rect(s, Inches(0.6), Inches(6.4), Inches(12.2), Emu(6350), fill=BORDER, line=None)
    text(s, Inches(0.6), Inches(6.55), Inches(8), Inches(0.4),
         "github.com/<org>/Iceshelf", size=14, color=CYAN, bold=True)
    text(s, Inches(10), Inches(6.55), Inches(2.8), Inches(0.4),
         "v1.0.0 \u00b7 Apache-2.0", size=14, color=TEXT, bold=True, align=PP_ALIGN.RIGHT)

# ---------- build ----------

builders = [
    s1_title, s2_problem, s3_what_it_is,
    s4_integration, s5_journey, s6_multi_region,
    s7_skills_agents, s8_security, s9_proof,
    s10_pilot, s11_next,
]
for fn in builders:
    fn()

total = len(prs.slides)
for i, sl in enumerate(prs.slides, start=1):
    footer(sl, i, total)

out = Path(__file__).parent / "presentation.pptx"
prs.save(out)
print(f"Wrote {out}  ({total} slides)")
