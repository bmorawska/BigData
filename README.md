## Przetwarzanie i analiza dużych zbiorów danych


#### Zadanie 1
Celem zadania było sprawdzenie jak radzą sobie metody nie-BigData, z dużymi zbiorami danych.
#### Zadanie 2
Program, który implementuje znany z mediów społecznościowych algorytm "Osoby, które możesz znać". Działać na tej zasadzie, że jeżeli dwóch użytkowników ma wielu wspólnych znajomych, to system proponuje im znajomość.
#### Zadanie 3
Napisz program, który implementuje algorytm k-średnich z uwzględnieniem dwóch miar: euklidesowej oraz Manhattan.
#### Zadanie 4
Program, który implementuje reguły asocjacyjne za pomocą algorytmu A-priori.
#### Projekt
Celem projektu jest zbadanie i graficzne podsumowanie wybranych cech zbioru danych dających odpowiedzi na część lub, jeśli to możliwe, na wszystkie poniższe pytania:
- Kiedy jest najlepsza pora dnia / dzień tygodnia / pora roku na lot, aby zminimalizować opóźnienia?
- Czy starsze samoloty mają większe opóźnienia?
- Jak zmienia się liczba osób podróżujących między różnymi lokalizacjami w czasie?
- Jak wpływa pogoda/prognoza pogody na opóźnienia samolotu?
- Czy możliwe jest znalezienie „wąskiego gardła” (ang. bottleneck), czyli miejsca, w którym opóźnienie na danym lotnisku wpływa znacząco na opóźnienia na innych lotniskach?
- Jaki wpływ na opóźnienia i częstotliwość lotów miał zamach terrorystyczny na World Trade Center w 2001 roku i pandemia koronawirusa w 2020 roku?

##### Dane
Dane pochodzą ze strony Ministerstwa Transportu Stanów Zjednoczonych.
Dane obejmują szczegóły przylotów i odlotów wszystkich lotów komercyjnych w USA, od stycznia 1995 r. do września 2020 r.
Każdy lot jest opisany maksymalnie przez 110 wartości, które pogrupowane są na następujące kategorie:
- podstawowe parametry (dzień, miesiąc, rok)
- dane dotyczące linii lotniczych
- dane dotyczące lotniska, z którego wyruszył samolot
- dane dotyczące lotniska docelowego
- dane dotyczące opóźnień przed lotem, wynikających z lotu i po przylocie
- przesunięcia, przekierowania lub likwidację lotu
- statystyk dotyczących lotu (czas lotu, pokonana odległość itp.)

Obecnie baza ma prawie 70 GB danych, a co miesiąc przybywa około 250 MB rekordów o 400 – 450 tysiącach lotów.

