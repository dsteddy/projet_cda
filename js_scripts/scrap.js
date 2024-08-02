const puppeteer = require('puppeteer');

async function scrap(jobTitle = "data", maxPage = 40) {
    const browser = await puppeteer.launch({ headless: true });
    const page = await browser.newPage();

    const customUa = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36';
    await page.setUserAgent(customUa);

    const apiLink = 'https://api.welcometothejungle.com/api/v1/organizations/';
    const results = [];
    let offersFound = false;

    try {
        for (let pageNumber = 1; pageNumber <= maxPage; pageNumber++) {
            // console.log('Page:', pageNumber)
            let url = `https://www.welcometothejungle.com/fr/jobs?refinementList%5Boffices.country_code%5D%5B%5D=FR&query=${jobTitle}&page=${pageNumber}`;

            try {
                await page.goto(url, { waitUntil: "networkidle0" });
                await page.setViewport({ width: 1080, height: 1024 });

                const links = await page.evaluate(() => {
                    const linkElements = document.querySelectorAll('a');
                    const hrefs = new Set();
                    linkElements.forEach(element => {
                        hrefs.add(element.href);
                    });
                    return Array.from(hrefs);
                });

                const jobUrlPattern = /\/companies\/[^\/]+\/jobs\/[^\/]+/;

                const jobOfferUrls = links.filter(url => jobUrlPattern.test(url));

                if (jobOfferUrls.length > 0) {
                    offersFound = true;
                    jobOfferUrls.forEach(link => {
                        const parts = link.split('/companies/');
                        if (parts.length === 2) {
                            const companyId = parts[1];
                            results.push(`${apiLink}${companyId}`);
                        }
                    });
                } else {
                    // console.info("Pas d'offre sur la page", pageNumber);
                    break;
                }

            } catch (error) {
                console.info("Erreur sur la page", pageNumber);
                await page.screenshot({ path: `error_page${pageNumber}.png`});
                break;
            }
        }

        if (!offersFound) {
            // console.info("Aucune offre trouvée pour cet intitulé", jobTitle);
        }
    } catch (error) {
        console.error("An error occured during scraping:", error);
    } finally {
        await browser.close();
    }

    return results;
}

(async () => {
    const jobTitle = process.argv[2] || 'data';
    const maxPage = parseInt(process.argv[3], 10) || 40;
    const results = await scrap(jobTitle, maxPage);
    console.log(JSON.stringify(results));
})();
