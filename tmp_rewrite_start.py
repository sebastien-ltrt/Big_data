from pathlib import Path
p = Path('start.bat')
text = p.read_text(encoding='utf-8-sig')
p.write_text(text, encoding='utf-8')
print('rewrote without BOM')
