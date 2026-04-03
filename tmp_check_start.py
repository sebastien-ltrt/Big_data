from pathlib import Path
p = Path('start.bat')
raw = p.read_bytes()
print('first bytes:', raw[:8])
print('first line repr:', p.read_text(encoding='utf-8', errors='replace').splitlines()[0])
