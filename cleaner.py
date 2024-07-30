from datetime import datetime
from bs4 import BeautifulSoup

class Cleaner:
    def __init__(self, offer_info):
        self.offer_to_clean: dict[str, str | float] = offer_info

    def clean_experience(self) -> dict[str, str | float]:
        job = self.offer_to_clean
        if 'experience' in job.keys():
            # < 6 mois
            if job["experience"] == None:
                job["experience"] = "6 mois"
            elif job["experience"] in ['Débutant accepté (0 YEAR)', 'LESS_THAN_6_MONTHS']:
                job["experience"] = "6 mois"
            elif job["experience"] == '6_MONTHS_TO_1_YEAR':
                job["experience"] = "6 mois"
            # 1 ans
            elif job["experience"] in ['Expérience exigée de 1 An(s)', '1_TO_2_YEARS']:
                job["experience"] = "1 an"
            # 2 ans
            elif job["experience"] in [
                'Expérience exigée de 2 An(s)',
                '24 mois',
                '2_TO_3_YEARS',
            ]:
                job["experience"] = "2 ans"
            # 3 ans
            elif job["experience"] in [
                'Expérience exigée de 3 An(s)',
                'Expérience souhaitée de 3 An(s)',
                '3_TO_4_YEARS',
                '36 mois',
            ]:
                job["experience"] = "3 ans"
            # 4 ans
            elif job["experience"] in [
                '4_TO_5_YEARS',
                'Expérience exigée de 4 An(s)',
            ]:
                job["experience"] = "4 ans"
            # 5 ans
            elif job["experience"] in [
                '5 ans - DATA ANALYST',
                '5 ans - 5 ans minimum',
                'Expérience exigée de 5 An(s)',
                '5_TO_7_YEARS',
            ]:
                job["experience"] = "5 ans"
            # 7 ans
            elif job["experience"] in [
                'Expérience exigée de 6 An(s)',
                '7_TO_10_YEARS',
            ]:
                job["experience"] = "5+ ans"
            # 10 ans
            elif job["experience"] == '10_TO_15_YEARS':
                job["experience"] = "10+ ans"
            # Non spécifié
            elif job["experience"] in [
                'Expérience exigée',
                'Expérience souhaitée',
            ]:
                job["experience"] = "Non spécifié"
        # Création de la key experience
        else:
            job["experience"] = "Non spécifié"

        return job

    def clean_etudes(self) -> dict[str, str | float]:
        job = self.offer_to_clean
        if 'niveau_etudes' in job.keys():
            text = job['niveau_etudes'].lower().strip()
            # Bac +5
            if text in ["bac_5", "bac+5"]:
                job['niveau_etudes'] = "Bac +5"
            # Bac +4
            if text in ["bac_4", "bac+4"]:
                job['niveau_etudes'] = "Bac +4"
            # Bac +3
            if text in ["bac_3", "bac+3"]:
                job['niveau_etudes'] = "Bac +3"
            # Bac +2
            if text in ["bac_2", "bac+2"]:
                job['niveau_etudes'] = "Bac +2"
        else:
            # Création de la key niveau_etudes
            job['niveau_etudes'] = "Non spécifié"

        return job

    def clean_desc(self) -> dict[str, str | float]:
        job = self.offer_to_clean
        if 'description' in job.keys():
            text = job['description']
            # Clean HTML
            soup = BeautifulSoup(str(text), 'html.parser')
            cleaned_text = soup.get_text(separator=" ")
            cleaned_text = cleaned_text.replace("\xa0", " ").replace("\n", "")
            job['description'] = cleaned_text
        else:
            # Création de la clé description
            job['description'] = ""
        # Check si la key description_entreprise est présent
        if 'description_entreprise' in job.keys():
            text = job['description_entreprise']
            # Clean HTML
            soup = BeautifulSoup(str(text), 'html.parser')
            cleaned_text = soup.get_text(separator=" ")
            cleaned_text = cleaned_text.replace("\xa0", " ").replace("\n", "")
            job['description_entreprise'] = cleaned_text
        else:
            # Création de la clé description_entreprise
            job['description_entreprise'] = ""

        return job

    def clean_dates(self) -> dict[str, str | float]:
        job = self.offer_to_clean
        date = job['date_publication']
        cleaned_date = datetime.strptime(date, '%Y-%m-%dT%H:%M:%SZ').strftime('%Y-%m-%d %H:%M')
        job['date_publication'] = cleaned_date

        date2 = job['date_modif']
        cleaned_date2 = datetime.strptime(date2, '%Y-%m-%dT%H:%M:%SZ').strftime('%Y-%m-%d %H:%M')
        job['date_modif'] = cleaned_date2

        return job

    def clean_salaire(self) -> dict[str, str | float]:
        job = self.offer_to_clean
        if 'salary_period' in job.keys():
            # Si le salaire annuel est indiqué
            if job['salary_period'] == "yearly":
                if 'salary_max' in job.keys() and 'salary_min' in job.keys():
                    salary = (job['salary_max'] + job['salary_min']) / 2
                    if salary < 100:
                        salary *= 1000
                    job['salary'] = salary
                    keys_to_remove = ['salary_period', 'salary_max', 'salary_min']
                    for key in keys_to_remove:
                        job.pop(key)
            # Si le salaire mensuel est indiqué
            elif job['salary_period'] == 'monthly':
                if 'salary_max' in job.keys() and 'salary_min' in job.keys():
                    salary_max = job['salary_max'] * 12
                    salary_min = job['salary_min'] * 12
                    salary = (salary_max + salary_min) / 2
                    if salary < 100:
                        salary *= 1000
                    job['salary'] = salary
                    keys_to_remove = ['salary_period', 'salary_max', 'salary_min']
                    for key in keys_to_remove:
                        job.pop(key)

        else:
            # Si la périodicité n'est pas indiqué (alors salaire mensuel -> salaire annuel)
            if 'salary_max' in job.keys() and 'salary_min' in job.keys():
                salary_max = job['salary_max'] * 12
                salary_min = job['salary_min'] * 12
                salary = (salary_max + salary_min) / 2
                if salary < 100:
                    salary *= 1000
                job['salary'] = salary
                keys_to_remove = ['salary_period', 'salary_max', 'salary_min']
                for key in keys_to_remove:
                    job.pop(key)

        # Création de la key salaire si manquante
        if 'salary' not in job.keys():
            job['salary'] = "Salaire non indiqué."

        return job

    def clean_columns(self) -> dict[str, str | float]:
        job = self.offer_to_clean
        cols_to_keep = [
        'id',
        'date_publication',
        'date_modif',
        'secteur_activite',
        'intitule',
        'entreprise',
        'logo',
        'description',
        'contrat',
        'description_entreprise',
        'ville',
        'latitude',
        'longitude',
        'experience',
        'niveau_etudes',
        'salary',
        ]

        # Drop des colonnes supplémentaires
        cols_to_drop = [col for col in job.keys() if col not in cols_to_keep]

        for drop in cols_to_drop:
            job.pop(drop)

        # Rajout des colonnes manquantes
        for key in cols_to_keep:
            if key not in job.keys():
                job[key] = "Non spécifié."

        # Reorder des colonnes
        reorder_job = {key: job[key] for key in cols_to_keep}

        return reorder_job

    def clean_full(self) -> dict[str, str | float]:
        job = self.offer_to_clean
        job = self.clean_experience()
        job = self.clean_etudes()
        job = self.clean_desc()
        job = self.clean_dates()
        job = self.clean_salaire()
        job = self.clean_columns()
        return job
