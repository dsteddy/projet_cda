const puppeteer = require('puppeteer');

async function scrap(jobTitle, maxPage) {
    const browser = await puppeteer.launch();
    const page = await browser.newPage();

    const customUa = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36';
    await page.setUserAgent(customUa);

    const apiLink = 'https://api.welcometothejungle.com/api/v1/organizations/';
    const results = [];
    let offersFound = false;

    for (let pageNumber = 1; pageNumber <= maxPage; pageNumber++) {
        let url = `https://www.welcometothejungle.com/fr/jobs?refinementList%5Boffices.country_code%5D%5B%5D=FR&query=${jobTitle}&page=${pageNumber}`;

        try {
            await page.goto(url, { waitUntil: "networkidle0" });
            await page.setViewport({ width: 1080, height: 1024 });

            const links = await page.evaluate(() => {
                const linkElements = document.querySelectorAll('a[backgroundsource]');
                const hrefs = [];
                linkElements.forEach(element => {
                    hrefs.push(element.href);
                });
                return hrefs;
            });

            if (links.length > 0) {
                offersFound = true;
                links.forEach(link => {
                    const parts = link.split('/companies/');
                    if (parts.length === 2) {
                        const companyId = parts[1];
                        results.push(`${apiLink}${companyId}`);
                    }
                });
            } else {
                console.log("Pas d'offre sur la page", pageNumber);
                break;
            }

        } catch (error) {
            console.log("Page non trouvée", pageNumber);
            break;
        }
    }

    if (!offersFound) {
        console.log("Aucune offre trouvée pour cet intitulé", jobTitle);
    }

    await browser.close();
    return results;
}

(async () => {
    const jobTitle = process.argv[2];
    const maxPage = parseInt(process.argv[3], 10);
    const results = await scrap(jobTitle, maxPage);
    console.log(JSON.stringify(results));
})();
