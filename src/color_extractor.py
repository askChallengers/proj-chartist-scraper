import cv2 as cv
import numpy as np
import requests
import colorsys
from sklearn.cluster import KMeans

def get_img_by_url(url: str):
    # URL에서 이미지 가져오기
    response = requests.get(url)
    response.raise_for_status()

    # 이미지를 numpy 배열로 변환
    image_array = np.asarray(bytearray(response.content), dtype=np.uint8)

    # OpenCV 형식으로 디코딩
    image = cv.imdecode(image_array, cv.IMREAD_COLOR)

    # BGR을 RGB로 변환 (matplotlib에서 사용하려면 필요)
    image_rgb = cv.cvtColor(image, cv.COLOR_BGR2RGB)
    return image_rgb

def identify_white_or_black(rgb):
    if all(value >= 200 for value in rgb):
        return "White-like"
    elif all(value <= 50 for value in rgb):
        return "Black-like"
    else:
        return "Neutral"
    
# Convert RGB to HEX
def rgb_to_hex(rgb):
    return "#{:02x}{:02x}{:02x}".format(int(rgb[0]), int(rgb[1]), int(rgb[2]))

def brighten_color(rgb, increment=50):
    return np.clip(np.array(rgb) + increment, 0, 255).astype(int)

def desaturate_color(rgb, factor=0.5):
    # Normalize RGB to [0, 1] range
    r, g, b = np.array(rgb) / 255.0
    # Convert to HLS (Hue, Lightness, Saturation)
    h, l, s = colorsys.rgb_to_hls(r, g, b)
    # Decrease saturation
    s *= factor
    # Convert back to RGB
    r, g, b = colorsys.hls_to_rgb(h, l, s)
    # Convert back to [0, 255] range
    return (np.array([r, g, b]) * 255).astype(int)

def get_dominant_color_by_url(url:str, cnt: int=3) -> dict:
    img = get_img_by_url(url)
    clt = KMeans(n_clusters=cnt, random_state=880818)
    clt.fit(img.reshape(-1, 3))
    centers = clt.cluster_centers_
    # Convert to integers
    init_rgb_colors = np.round(centers).astype(int)
    rgb_colors = []
    for _rgb in init_rgb_colors:
        if identify_white_or_black(_rgb) != "Neutral":
            continue
        rgb_colors += [_rgb]
    hex_colors = [rgb_to_hex(color) for color in rgb_colors]
    # print("Org. Colors(RGB):", rgb_colors)
    # print("Org. Colors(HEX):", hex_colors)
    
    pastel_colors = []
    for color in rgb_colors:
        brightened = brighten_color(color, increment=25)  # Step 1: Brighten
        pastel = desaturate_color(brightened, factor=0.6) # Step 2: Reduce Saturation
        pastel_colors.append(pastel)

    hex_pastel_colors = [rgb_to_hex(color) for color in pastel_colors]
    # print("Pastel Colors (RGB):", pastel_colors)
    # print("Pastel Colors (HEX):", hex_pastel_colors)
    result_colors = hex_pastel_colors + [None] * (cnt - len(hex_pastel_colors))

    result = {f'color_{idx+1}': item for idx, item in enumerate(result_colors)}
    result.update({'img_url': url})
    return result