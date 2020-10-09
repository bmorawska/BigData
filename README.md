## Przetwarzanie i analiza dużych zbiorów danych


#### Zadanie 1
Pobierz dane dotyczące zgłoszeń do władz Nowego Jorku za pośrednictwem numeru 3-1-1:
https://nycopendata.socrata.com/Social-Services/311-Service-Requests-from-2010-to-Present/erm2-nwe9

1.  Na podstawie pliku CSV znajdź:
    - najczęściej zgłaszane skargi,
    - najczęściej zgłaszane skargi w każdej dzielnicy,
    - urzędy, do których najczęściej zgłaszano skargi.
    Zmierz czasy wykonania kwerend.

2.  Wczytaj dane z pliku CSV do bazy danych SQL i ponownie wykonaj trzy powyższe kwerendy.
    - Zmierz czasy wykonania kwerend.
    - Zmierz łączny czas potrzebny na przekonwertowanie danych oraz wykonanie kwerend.
    - Porównaj te czasy dla dwóch różnych baz danych (np. MySQL oraz SQLite).

    Ważne! Do łączenia się z bazą danych oraz wykonania kwerend wykorzystaj język Python.

3.  W jaki sposób można zredukować czas wykonania kwerend? Zaimplementuj własne rozwiązanie i przedstaw wyniki.

Ważne! Wykonaj zadanie również wtedy, gdy osiągane rezultaty nie są lepsze od wyjściowych.