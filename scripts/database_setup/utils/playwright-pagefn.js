// See https://apify.com/apify/playwright-scraper#page-function for details

async function pageFunction(context) {   
    const html = await context.page.content();
    const googleUserRatingLoc = context.page.locator('[data-attrid$="thumbs_up"]')
    const allRatingsLoc = context.page.locator('[data-attrid$="reviews"]')
    
    // Store HTML for the Google user review element
    const googleUserRating = await googleUserRatingLoc.count() > 0 ? 
        await googleUserRatingLoc.evaluate(el => el.outerHTML)
        : null;
    // Store array of HTML for all the "reviews" elements (returns an empty array if no elements are found)
    const allRatings = await allRatingsLoc.count() > 0
        ? await allRatingsLoc.evaluateAll(els => els.map(el => el.outerHTML))
        : [];
    
    // If the Google user rating isn't present, we're going to try alternative search paths
    if(!googleUserRating){
        let url = context.customData["alt_search_paths"].shift() // TODO not sure if customData is mutable
        await context.enqueueRequest({ url });
    }
    else{
        allRatings.push(googleUserRating);
    }

    context.log.info(JSON.stringify(context.request,null,2))
    // Return an object with the data extracted from the page.
    // It will be stored to the resulting dataset.
    return {
        url: context.request.url,
        loadedUrl: context.request.loadedUrl,
        headers: context.request.headers,
        proxyInfo: context.proxyInfo,
        googleUserRating,
        allRatings,
        html,
        env: context.env,
    };
}