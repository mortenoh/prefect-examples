# CLAUDE.md

## Commit conventions

- Use conventional commits (e.g. `feat:`, `fix:`, `docs:`, `chore:`, `refactor:`, `test:`)
- No Co-Authored-By or other attribution lines
- Never use emojis anywhere — not in commits, code, comments, or responses

## Tooling

- Always use `uv` for package management -- never `pip`

## Flow conventions

- All flow files must include `load_dotenv()` as the first line inside their
  `if __name__ == "__main__":` block so that `uv run python <file>` loads `.env`
  automatically. Add `from dotenv import load_dotenv` in the third-party import
  section.
