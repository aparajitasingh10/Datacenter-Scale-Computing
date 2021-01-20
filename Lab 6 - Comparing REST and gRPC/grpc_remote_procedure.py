from PIL import Image
import io

def add(a, b):
    y = a + b
    return y

def image(x):
    ioBuffer = io.BytesIO(x)
    img = Image.open(ioBuffer)
    return img.size[0], img.size[1]


