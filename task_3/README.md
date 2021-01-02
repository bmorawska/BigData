## Przetwarzanie i analiza dużych zbiorów danych


#### Zadanie 3
1. Napisz program, który implementuje algorytm k-średnich z uwzględnieniem dwóch miar: euklidesowej oraz Manhattan.

Ważne! W tym celu skorzystaj z Apache Spark oraz języka Python.

Pseudokod
```
procedure Iteracyjny algorytm k-średnich
    Wybierz k wektorów jako początkowe centroidy k klastrów.
    for iteracje := 1 to MAX_ITERACJI do
        for każdy wektor x do
            Przypisz wektor x do klastra o najbliższym centroidzie.
        end for
        Oblicz koszt iteracji.
        for każdy klaster c do
            Oblicz nowy centroid klastra c jako średnią ze wszystkich wektorów do niego przypisanych.
        end for
    end for
end procedure
```

Ważne! Algorytm należy zaimplementować samodzielnie. Nie wolno korzystać z gotowych rozwiązań.

Pliki wejściowe
- plik 3a.txt zawiera zbiór danych, który posiada 4601 wektorów o 58 kolumnach
- plik 3b.txt zawiera 10 początkowych centroidów, które zostały losowo wybrane spośród zbioru danych
- plik 3c.txt zawiera 10 początkowych centroidów, które są od siebie maksymalnie oddalone według odległości euklidesowej

Parametry
- liczba klastrów k powinna liczyć 10
- maksymalna liczba iteracji MAX_ITERACJI ma wynosić 20

2. Dla obydwu początkowych rozmieszczeń centroidów oblicz funkcje kosztu ϕ(i) oraz ψ(i) dla każdej iteracji i.

3. Wygeneruj wykresy ϕ(i) oraz ψ(i). Tutaj również uwzględnij obydwa początkowe rozmieszczenia centroidów.

4. Jak procentowo zmienił się koszt po 10 iteracjach algorytmu dla obydwu miar odległości? Dla którego z początkowych rozmieszczeń centroidów uzyskano lepsze rezultaty?

Ważne! W tym celu podziel różnicę między kosztem początkowym oraz tym uzyskanym po 10. iteracji przez koszt początkowy.