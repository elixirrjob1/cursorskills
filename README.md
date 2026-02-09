# Cursor Skills Repository

A collection of reusable Cursor AI skills that enhance your development workflow.

## What are Cursor Skills?

Cursor Skills are reusable capabilities that extend Cursor's AI assistant with specialized knowledge and tools. Each skill is self-contained and can be easily shared across projects.

## Available Skills

### Database Analyzer

Analyzes database schemas and produces fully enriched JSON files with comprehensive metadata.

**Features:**
- Complete schema analysis (tables, columns, primary keys, foreign keys)
- Enriched metadata (row counts, field classifications, sensitive fields detection)
- ETL support (incremental columns, partition columns, CDC status)
- Multi-database support (PostgreSQL, easily extensible)

See [`.cursor/skills/database-analyser/README.md`](.cursor/skills/database-analyser/README.md) for details.

## Installation

### Quick Install

Copy the entire `.cursor/skills/` folder to your project:

```bash
# Copy skills folder to your project (project-level)
cp -r .cursor/skills /path/to/your/project/.cursor/

# Or copy to your home directory for global access (available across all projects)
cp -r .cursor/skills ~/.cursor/
```

### Individual Skill Installation

If you only want a specific skill:

```bash
# Copy a specific skill to your project
cp -r .cursor/skills/database-analyser /path/to/your/project/.cursor/skills/

# Or copy to your home directory for global access
cp -r .cursor/skills/database-analyser ~/.cursor/skills/
```

## Usage

Once installed, skills are automatically available in Cursor. Simply ask Cursor to perform tasks related to the skill's capabilities, and it will use the skill automatically.

### Example: Database Analysis

```bash
# Ask Cursor: "I want to analyze a database"
# Or run manually:
python .cursor/skills/database-analyser/scripts/database_analyzer.py \
  "postgresql://user:pass@host/db" \
  schema.json \
  public
```

## Project Structure

```
.cursor/
  └── skills/
      └── database-analyser/
          ├── README.md          # Skill-specific documentation
          ├── SKILL.md           # Skill instructions for AI
          └── scripts/
              └── database_analyzer.py
```

## Contributing

To add a new skill:

1. Create a new folder under `.cursor/skills/`
2. Include:
   - `README.md` - User-facing documentation
   - `SKILL.md` - AI instructions (see [create-skill guide](https://cursor.sh/docs/skills))
   - Any necessary scripts or resources

## Requirements

Skills may have their own requirements. Check each skill's README for details.

**Common requirements:**
- Python 3.7+ (includes venv module)
- Virtual environment (recommended for Python-based skills)
- Various Python packages (see individual skill READMEs)

## License

[Add your license here]
