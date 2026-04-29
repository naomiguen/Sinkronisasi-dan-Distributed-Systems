from PIL import Image, ImageDraw, ImageFont
import sys
import textwrap

def read_text_with_fallback(path):
    for enc in ('utf-8', 'utf-16', 'utf-16-le', 'latin-1'):
        try:
            with open(path, 'r', encoding=enc, errors='replace') as f:
                return f.read()
        except Exception:
            continue
    # last resort
    with open(path, 'rb') as f:
        return f.read().decode('latin-1', errors='replace')

def render_text_to_image(input_path, output_path, max_width=100, padding=10, font_size=14):
    text = read_text_with_fallback(input_path)

    wrapper = textwrap.TextWrapper(width=max_width)
    lines = []
    for paragraph in text.splitlines():
        if paragraph.strip() == "":
            lines.append("")
            continue
        wrapped = wrapper.wrap(paragraph)
        if not wrapped:
            lines.append("")
        else:
            lines.extend(wrapped)

    font = ImageFont.load_default()

    # estimate size using a temporary image
    temp_img = Image.new('RGB', (10, 10), color='white')
    draw_temp = ImageDraw.Draw(temp_img)
    # measure height of one line
    bbox = draw_temp.textbbox((0,0), 'A', font=font)
    line_height = (bbox[3] - bbox[1]) + 2
    img_width = min(2000, (max_width * 7) + padding * 2)
    img_height = line_height * (len(lines) + 1) + padding * 2

    img = Image.new('RGB', (img_width, max(64, img_height)), color='white')
    draw = ImageDraw.Draw(img)

    y = padding
    for line in lines:
        draw.text((padding, y), line, fill='black', font=font)
        y += line_height

    img.save(output_path)

if __name__ == '__main__':
    if len(sys.argv) < 3:
        print('Usage: render_text_image.py <input.txt> <output.png>')
        sys.exit(2)
    render_text_to_image(sys.argv[1], sys.argv[2])
