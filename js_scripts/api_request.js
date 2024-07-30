const axios = require('axios');

async function apiRequest(link) {
    const keysToKeep = [
        { key: "reference", newKey: "id" },
        { key: "published_at", newKey: "date_publication" },
        { key: "contract_type", newKey: "contrat" },
        { key: "name", newKey: "intitule" },
        { key: "description" },
        { key: "organization.industry", newKey: "secteur_activite" },
        { key: "education_level", newKey: "niveau_etudes" },
        { key: "salary_period" },
        { key: "organization.name", newKey: "entreprise" },
        { key: "organization.description", newKey: "description_entreprise" },
        { key: "office.city", newKey: "ville" },
        { key: "link" },
        { key: "organization.logo.url", newKey: "logo" },
        { key: "salary_min" },
        { key: "salary_max" },
        { key: "experience_level", newKey: "experience" },
        { key: "updated_at", newKey: "date_modif" },
        { key: "office.latitude", newKey: "latitude" },
        { key: "office.longitude", newKey: "longitude" },
    ];

    let jobOffer = {};

    try {
        const response = await axios.get(link);
        const jobData = response.data.job;

        for (const item of keysToKeep) {
            const { key, newKey } = item;
            const nestedKeys = key.split('.');
            let nestedValue = jobData;

            for (const nestedKey of nestedKeys) {
                if (nestedValue.hasOwnProperty(nestedKey)) {
                    nestedValue = nestedValue[nestedKey];
                } else {
                    nestedValue = null;
                    break;
                }
            }


            if (nestedValue !== null) {
                jobOffer[newKey || key] = nestedValue;
            }
        }
    } catch (error) {
        console.error("Error fetching job data:", error);
    }

    return jobOffer;
}

(async () => {
    const link = process.argv[2];
    jobOffer = await apiRequest(link);
    console.log(JSON.stringify(jobOffer));
})();