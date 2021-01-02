## Przetwarzanie i analiza dużych zbiorów danych


#### Zadanie 2
1. Napisz program, który implementuje znany z mediów społecznościowych 
algorytm "Osoby, które możesz znać". Powinien on działać na zasadzie, 
że jeżeli dwóch użytkowników ma wielu wspólnych znajomych, to system 
proponuje im znajomość.

Ważne! W tym celu skorzystaj z Apache Spark oraz języka Python.

Algorytm
- dla każdego użytkownika program powinien polecić mu 10 nowych 
użytkowników, którzy nie są jego znajomymi, ale którzy mają z nim najwięcej wspólnych znajomych

Plik wejściowy
- plik zawiera linie w formacie <UŻYTKOWNIK><TABULATOR><ZNAJOMI>, 
gdzie <UŻYTKOWNIK> oznacza unikalny identyfikator użytkownika, 
a <ZNAJOMI> to oddzielone po przecinku identyfikatory znajomych 
użytkownika o identyfikatorze <UŻYTKOWNIK>

Plik wyjściowy
- plik powinien zawierać linie w formacie 
<UŻYTKOWNIK><TABULATOR><REKOMENDACJE>, gdzie <UŻYTKOWNIK> 
oznacza unikalny identyfikator użytkownika, a <REKOMENDACJE> 
to oddzielone po przecinku identyfikatory użytkowników, którzy 
są rekomendowani jako znajomi użytkownikowi o identyfikatorze 
<UŻYTKOWNIK>

Ważne! Rekomendacje powinny zostać posortowane malejąco według liczby 
wspólnych znajomych. Jeżeli rekomendowani użytkownicy mają tylu samych
 wspólnych znajomych, to należy ich posortować rosnąco według ich 
 identyfikatora. Rekomendacje należy przedstawić również wtedy, 
 gdy jest ich mniej niż 10. Jeżeli użytkownik nie ma znajomych, 
 to wynikiem powinna być pusta lista.

2. Opisz w maksymalnie 3 zdaniach, w jaki sposób skorzystałeś z 
Apache Spark, aby rozwiązać zadanie.

3. Przedstaw rekomendacje dla użytkowników o identyfikatorach: 
924, 8941, 8942, 9019, 9020, 9021, 9022, 9990, 9992, 9993.

Ważne! Przydatne może okazać się polecenie .take(X).

Podpowiedź
- rekomendacje dla użytkownika o identyfikatorze 11 to: 27552,7785,
27573,27574,27589,27590,27600,27617,27620,27667ież wtedy, gdy 
osiągane rezultaty nie są lepsze od wyjściowych.