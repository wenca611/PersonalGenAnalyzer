"""
Generátor a analyzátor osobních údajů s detekcí shod a oslavenců

9742
https://www.mvcr.cz/clanek/seznam-rodove-neutralnich-jmen.aspx

280399
https://web.archive.org/web/20160307034828/http://www.mvcr.cz/clanek/
cetnost-jmen-a-prijmeni-722752.aspx?q=Y2hudW09MQ%3D%3D


https://cs.wikipedia.org/wiki/Akademick%C3%BD_titul
"""
import pandas as pd
import random as rnd
from faker import Faker
import time
import string
import os
from fuzzywuzzy import fuzz
import re
from unidecode import unidecode
from pandasgui import show

class Timer:
    def __enter__(self):
        # Uložíme čas na začátku
        self.start_time = time.time()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        # Uložíme čas na konci
        end_time = time.time()
        # Vypočítáme a vypíšeme dobu běhu
        elapsed_time = end_time - self.start_time
        print(f"Elapsed time: {elapsed_time:.4f} seconds")

class DataProcessor:
    def __init__(self):
        self.first_names = pd.DataFrame()
        self.last_names = pd.DataFrame()
        self.filtered_first_names = pd.DataFrame()
        self.filtered_last_names = pd.DataFrame()
        self.titles = pd.DataFrame(columns=['Title'])
        titles_list = [
            "Bc.", "BcA.", "Ing.", "Ing. arch.", "MUDr.", "MDDr.", "MVDr.", "MgA.", "Mgr.",
            "JUDr.", "PhDr.", "RNDr.", "PharmDr.", "ThLic.", "ThDr.",
            "akad. arch.", "ak. mal.", "ak. soch.", "MSDr.", "PaedDr.",
            "PhMr.", "RCDr.", "RSDr.", "RTDr.", "ThMgr.",
            "Ph.D.", "DSc.", "CSc.", "Dr.", "DrSc.", "Th.D.", "as.", "odb. as.", "doc.", "prof."
        ]

        # Převod seznamu titulů na DataFrame
        self.titles = pd.DataFrame(titles_list, columns=['Title'])

    def load_first_name(self, *files):
        all_names = []

        for file in files:
            # Načtěte všechny listy v Excel souboru
            xls = pd.ExcelFile(file)

            # Pro každý list v souboru
            for sheet_name in xls.sheet_names:
                df = pd.read_excel(xls, sheet_name=sheet_name, header=2)

                # Vyhledejte sloupce, které začínají na 'Jména'
                name_columns = [col for col in df.columns if col.startswith('Jména')]

                if name_columns:
                    # Zpracujte každý relevantní sloupec
                    for col in name_columns:
                        # Vyberte pouze sloupec s názvem 'Jména' a vynechte prázdné hodnoty
                        names = df[[col]].dropna()
                        if not names.empty:
                            names = names.rename(columns={col: 'Name'})  # Přejmenujte sloupec pro sjednocení
                            all_names.append(names)
                else:
                    print(f"Warning: No columns starting with 'Jména' found in sheet '{sheet_name}' of {file}.")

        # Sloučení všech nalezených sloupců do jednoho DataFrame
        if all_names:
            self.first_names = pd.concat(all_names, ignore_index=True)
            # print(self.first_names)
        else:
            print("No name columns found in any of the provided files.")

    def load_last_name(self, *files):
        all_last_names = []

        for file in files:
            # Načtěte všechny listy v Excel souboru
            xls = pd.ExcelFile(file)

            # Pro každý list v souboru
            for sheet_name in xls.sheet_names:
                df = pd.read_excel(xls, sheet_name=sheet_name)

                # Vyhledejte sloupec s názvem 'PŘÍJMENÍ'
                if 'PŘÍJMENÍ' in df.columns:
                    # Vyberte sloupec s názvem 'PŘÍJMENÍ' a vynechte prázdné hodnoty
                    last_names = df[['PŘÍJMENÍ']].dropna()
                    if not last_names.empty:
                        last_names = last_names.rename(
                            columns={'PŘÍJMENÍ': 'LastName'})  # Přejmenujte sloupec pro sjednocení
                        all_last_names.append(last_names)
                else:
                    print(f"Warning: 'PŘÍJMENÍ' column not found in sheet '{sheet_name}' of {file}.")

        # Sloučení všech nalezených sloupců do jednoho DataFrame
        if all_last_names:
            self.last_names = pd.concat(all_last_names, ignore_index=True)

            # Odstranit poslední řádek, pokud existuje
            if not self.last_names.empty:
                self.last_names = self.last_names[:-1]
        else:
            print("No 'PŘÍJMENÍ' columns found in any of the provided files.")

    @staticmethod
    def count_name_variants(data_frame, column_name):
        if not data_frame.empty:
            # Rozdělit názvy na jednotlivá slova
            split_names = data_frame[column_name].str.split(expand=True)

            # Zjistit, která jména mají více než jedno slovo
            name_lengths = split_names.apply(lambda row: row.notna().sum(), axis=1)
            multiple_names = data_frame[name_lengths > 1]

            single_names_count = len(data_frame) - len(multiple_names)
            multiple_names_count = len(multiple_names)

            print(f"Single {column_name.lower()} count: {single_names_count}")
            print(f"Multiple {column_name.lower()} count: {multiple_names_count}")

            # # Vypsat složená jména/příjmení
            # if not multiple_names.empty:
            #     print(f"{column_name} that are composed of multiple words:")
            #     for name in multiple_names[column_name]:
            #         print(f"- {name}")
            # else:
            #     print(f"No {column_name.lower()} are composed of multiple words.")
        else:
            print(f"No data available for {column_name}.")

    def count_first_name(self):
        self.count_name_variants(self.first_names, 'Name')

    def count_last_name(self):
        self.count_name_variants(self.last_names, 'LastName')

    @staticmethod
    def remove_multiple_word_entries(data_frame, column_name):
        """
        Removes entries from the DataFrame where the number of words in the specified column is greater than 1.

        :param data_frame: The DataFrame to process.
        :param column_name: The column name to check.
        :return: A DataFrame with entries containing more than one word removed.
        """
        if not data_frame.empty:
            # Rozdělit názvy na jednotlivá slova
            split_names = data_frame[column_name].str.split(expand=True)

            # Zjistit, která jména mají více než jedno slovo
            name_lengths = split_names.apply(lambda row: row.notna().sum(), axis=1)
            filtered_data = data_frame[name_lengths == 1]

            # Resetovat indexy
            filtered_data = filtered_data.reset_index(drop=True)

            return filtered_data
        else:
            print(f"No data available for {column_name}.")
            return pd.DataFrame()  # Vraťte prázdný DataFrame, pokud není k dispozici žádný obsah

    def remove_multiple_first_name(self):
        self.filtered_first_names = self.remove_multiple_word_entries(self.first_names, 'Name')

    def remove_multiple_last_name(self):
        self.filtered_last_names = self.remove_multiple_word_entries(self.last_names, 'LastName')

    def generate_full_name(self, title, first_name, last_name):
        full_names = [f"{first_name} {last_name}", f"{first_name.capitalize()} {last_name}",
                      f"{first_name} {last_name.capitalize()}",
                      f"{first_name.capitalize()} {last_name.capitalize()}",
                      f"{first_name[0]}. {last_name}", f"{first_name} {last_name[0]}."]

        full_names += [name.replace(" ", ", ") for name in full_names]
        full_names += [f"{title[0]} {name}" for name in full_names] + [f"{name}, {title[0]}" for name in full_names]
        full_names += [f"{name}, {title[1]}" for name in full_names]
        full_names += [f"{name}, {title[2]}" for name in full_names]

        return rnd.choice(full_names)

    @staticmethod
    def random_char():
        return rnd.choice(string.ascii_letters + string.digits + string.punctuation)

    def apply_random_changes(self, full_name, max_changes=3):
        name = list(full_name)  # Převedení na seznam pro snadnou manipulaci
        changes_made = 0

        # 1% šance na úpravu písmen
        if rnd.random() < 0.01:
            for _ in range(rnd.randint(1, max_changes)):
                if changes_made >= max_changes:
                    break
                pos = rnd.randint(0, len(name) - 1)
                if name[pos].isalpha():  # Změna pouze písmen
                    name[pos] = self.random_char()
                    changes_made += 1

        # 1% šance na úpravu teček a čárek
        if rnd.random() < 0.01:
            for _ in range(rnd.randint(1, max_changes)):
                if changes_made >= max_changes:
                    break
                pos = rnd.randint(0, len(name) - 1)
                if name[pos] in '.':
                    name[pos] = ','
                    changes_made += 1
                elif name[pos] in ',':
                    name[pos] = '.'
                    changes_made += 1

        # 1% šance na úpravu mezer
        if rnd.random() < 0.01:
            for _ in range(rnd.randint(1, max_changes)):
                if changes_made >= max_changes:
                    break
                pos = rnd.randint(0, len(name) - 1)
                if name[pos] == ' ':
                    # Nahrazení mezery dvěma nebo více mezerami
                    extra_spaces = rnd.randint(1, 3)  # 1 až 3 extra mezery
                    name[pos] = ' ' * extra_spaces
                    changes_made += 1

        return ''.join(name)

    def generate_data(self, num_records, output_file):
        # Initialize Faker for generating random dates
        fake = Faker()

        records = []

        for i in range(num_records):
            id_ = i + 1
            first_name1, first_name2 = rnd.sample(self.filtered_first_names['Name'].tolist(), 2)
            last_name1, last_name2 = rnd.sample(self.filtered_last_names['LastName'].tolist(), 2)

            # Randomly choose titles and positions
            titles1 = self.titles['Title'].sample(n=3, replace=False).tolist()
            titles2 = self.titles['Title'].sample(n=3, replace=False).tolist()

            full_name1 = self.generate_full_name(titles1, first_name1, last_name1)
            full_name2 = self.generate_full_name(titles2, first_name2, last_name2)

            full_name1 = self.apply_random_changes(full_name1)
            full_name2 = self.apply_random_changes(full_name2)

            # print("Dvojice jmen:", full_name1, " x ", full_name2)

            # Generate random dates of birth
            born_date1 = fake.date_of_birth(minimum_age=18, maximum_age=90).strftime('%Y-%m-%d')
            born_date2 = fake.date_of_birth(minimum_age=18, maximum_age=90).strftime('%Y-%m-%d')

            record = {
                'ID': id_,
                'JMENO1': full_name1,
                'JMENO2': full_name2,
                'DATUM1': born_date1,
                'DATUM2': born_date2
            }

            records.append(record)

        df = pd.DataFrame(records)
        df.to_csv(output_file, index=False)
        print(f"Data has been saved to {output_file}.")

    def load_and_process_data(self, input_file):
        # Načtení souboru, pokud existuje
        if os.path.exists(input_file):
            # Načtení dat ze souboru
            data = pd.read_csv(input_file)

            # Zkontrolujeme, zda obsahuje potřebné sloupce
            required_columns = {'ID', 'JMENO1', 'JMENO2', 'DATUM1', 'DATUM2'}
            if not required_columns.issubset(data.columns):
                print("Error: Input file is missing required columns.")
                return

            # Funkce pro odstranění titulu z jména
            def remove_title(name):
                # Rozdělení jména na části
                parts = name.split()
                # print("parts:", parts)
                filtered_parts = []
                non_filtered_parts = []
                for part in parts:
                    is_title = any(
                        fuzz.partial_ratio(part.lower(), title.lower()) > 80 for title in self.titles['Title'].tolist())
                    if not is_title:
                        filtered_parts.append(part)
                    else:
                        non_filtered_parts.append(part)

                # Zajistit alespoň dvě slova
                if len(filtered_parts) < 2:
                    # Pokud chybí druhé slovo, přidáme slova s menší podobností k titulu
                    # Seřadíme podle podobnosti k titulu
                    sorted_non_filtered_parts = sorted(non_filtered_parts, key=lambda part: max(
                        fuzz.partial_ratio(part.lower(), title.lower()) for title in
                        self.titles['Title'].tolist()),
                                                       reverse=True)

                    # Přidáme chybějící slova, dokud nebude mít text alespoň dvě slova
                    while len(filtered_parts) < 2 and sorted_non_filtered_parts:
                        filtered_parts.append(sorted_non_filtered_parts.pop(0))

                    # Pokud stále chybí slova, přidáme defaultní hodnotu
                    if len(filtered_parts) < 2:
                        filtered_parts = ['Unknown', 'Unknown']

                # Spojení části bez titulů do jednoho řetězce
                filtered_name = ' '.join(filtered_parts)

                # Odstranění diakritiky
                filtered_name = unidecode(filtered_name)

                # Odstranění všech nežádoucích znaků (nepísmena a mezery) a vícenásobných mezer
                filtered_name = re.sub(r'[^a-zA-Z\s]', '', filtered_name)  # Odstraní vše kromě písmen a mezer
                filtered_name = re.sub(r'\s+', ' ', filtered_name)  # Zredukuje vícenásobné mezery na jednu
                filtered_name = filtered_name.lower()
                # print("Cleaned name:", filtered_name)  # Debugging line

                return filtered_name

            # Rozdělení JMENO1 na části a odstranění titulu
            data['JMENO1'] = data['JMENO1'].apply(lambda x: remove_title(x))

            # Rozdělení JMENO2 na části a odstranění titulu
            data['JMENO2'] = data['JMENO2'].apply(lambda x: remove_title(x))

            # Výpis výsledků
            print("Processed data:")
            print(data[['ID', 'JMENO1', 'JMENO2']])

            # Uložení upravených dat
            output_file = 'processed_' + os.path.basename(input_file)
            data.to_csv(output_file, index=False)
            print(f"Processed data has been saved to {output_file}.")
        else:
            print(f"File {input_file} does not exist.")

    @staticmethod
    def display_filtered_data(input_file):
        # Načtení dat ze souboru
        data = pd.read_csv(input_file)

        # Zkontrolujeme, zda obsahuje potřebné sloupce
        if 'JMENO1' not in data.columns or 'JMENO2' not in data.columns:
            print("Error: Input file is missing required columns.")
            return

        # Filtrace přesné shody
        exact_match = data[data['JMENO1'] == data['JMENO2']]

        # Filtrace částečné shody (fuzzy match)
        def is_fuzzy_match(row):
            return fuzz.partial_ratio(row['JMENO1'].lower(), row['JMENO2'].lower()) > 80

        fuzzy_match = data[data.apply(is_fuzzy_match, axis=1)]

        # Zobrazení dat v pandasgui
        gui = show([exact_match, fuzzy_match],
                   ["Exact Match", "Fuzzy Match"])

if __name__ == "__main__":
    proc = DataProcessor()
    proc.load_first_name("Seznam_muzskych_jmen_-_20240731.xlsx",
                         "Seznam_rodove_neutralnich_jmen_-_20240618.xlsx",
                         "Seznam_zenskych_jmen_-_20240731.xlsx")
    proc.load_last_name("stobyv_20160202.xls")
    # print(processor.first_names)
    # print(processor.last_names)
    proc.count_first_name()
    proc.count_last_name()
    proc.remove_multiple_first_name()
    proc.remove_multiple_last_name()
    # print(proc.filtered_first_names)
    # print(proc.filtered_last_names)

    # Generování a uložení dat
    proc.generate_data(num_records=50, output_file='generated_data.csv')

    proc.load_and_process_data(input_file='generated_data.csv')

    proc.display_filtered_data(input_file='processed_generated_data.csv')

