import asyncio
from requests_html import AsyncHTMLSession

asession = AsyncHTMLSession()

async def fetch_js_rendered():
    proxies = {
        'http': 'http://zlseuhhi:hea1bv9cvqnj@198.23.239.134:6540',
        'https': 'http://zlseuhhi:hea1bv9cvqnj@198.23.239.134:6540',
    }

    r = await asession.get(
        'https://rv.campingworld.com/shop-rvs?indexName=rvcw-inventory_recommended',
        proxies=proxies,
        timeout=30
    )

    await r.html.arender(timeout=30)
    return r.html.html

if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    html = loop.run_until_complete(fetch_js_rendered())

    # Write HTML to a file
    with open('page.html', 'w', encoding='utf-8') as f:
        f.write(html)

    print("HTML saved to page.html")
