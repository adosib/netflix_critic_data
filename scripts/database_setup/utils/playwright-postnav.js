// https://apify.com/apify/playwright-scraper/input-schema#postNavigationHooks
[
    async (crawlingContext) => {
        crawlingContext.log.info(JSON.stringify(crawlingContext.request, null, 2));
        crawlingContext.log.info(JSON.stringify(crawlingContext.response, null, 2));
        crawlingContext.log.info(JSON.stringify(crawlingContext.proxyInfo, null, 2));
    },
]