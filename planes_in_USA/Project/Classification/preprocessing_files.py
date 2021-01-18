import os

directory_with_files = 'path'  # tu wpisać ścieżkę do katalogu z plikami

my_files_list = os.listdir(directory_with_files)
string_to_contain = '2020'  # a tutaj coś charakterystycznego, co mają wszystkie te pliki np. rok
files = []
for filename in my_files_list:
    if string_to_contain in filename:
        files.append(filename)


new_csv_name = '2020_planes_data.csv' # to jest nazwa nowego pliku csv
f = open(new_csv_name, 'a')

with open(os.path.join(directory_with_files, files[0]), 'r') as k:
    lines = k.readlines()
f.write(lines[0])

for file in files:
    with open(os.path.join(directory_with_files, file), 'r') as k:
        lines = k.readlines()
    lines = lines[1:] # usuwamy nagłowek
    for l in lines:
        f.write(l)
    f.write('\n')

print(f"Twój pliczek {new_csv_name} jest gotowy")
