from scrapling import Fetcher
from scrapling.fetchers import DynamicFetcher


url = 'https://www.emol.com/noticias/Nacional/2026/07/24/1206634/instituto-nacional-incidentes-molotovs-adultos.html'


page = DynamicFetcher.fetch(url, headless=False)
titulo_noticia = page.css(
    "h1#cuDetalle_cuTitular_tituloNoticia")[0].text
bajada_noticia = page.css(
    "h2#cuDetalle_cuTitular_bajadaNoticia")[0].text
texto_noticia = "\n".join([
                tag.get_all_text(strip=True)
                for tag in page.css(
                    "div#cuDetalle_cuTexto_textoNoticia > div"
                )
    if tag.get_all_text(strip=True)
])

# Get the comments (only the first )
comment_container = page.css('div#cont_comment')
print("Comment container")
print(comment_container)

comments = page.css(
    'div#cont_comment > div.margin_lvl_0 > div.cmtbubble'
)
print("Comments")
print(comments)

comments_parse = [
    c.css('div.cmtdata > div.cm_txt_data')[0].text for c in comments
]
print("Comments parse")
print(comments_parse)
