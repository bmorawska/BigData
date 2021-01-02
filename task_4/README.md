## Przetwarzanie i analiza dużych zbiorów danych


#### Zadanie 4
1. Napisz program, który implementuje reguły asocjacyjne za pomocą algorytmu A-priori. Program powinien - na podstawie ostatnio przeglądanych przez użytkowników przedmiotów - identyfikować te, które regularnie występowały wspólnie w ramach pojedynczej sesji.

Ważne! W tym celu skorzystaj z Apache Spark oraz języka Python.

Miary istotności
Załóżmy, że X i Y to zbiory elementów transakcji, t oznacza transakcję, a T jest zbiorem transakcji.
- wsparcie (ang. support): supp(X)=|{t∈T:X⊆t}||T|
- ufność (ang. confidence): conf(X⇒Y)=supp(X∪Y)supp(X)

Algorytm
- program powinien znaleźć wszystkie pary i trójki przedmiotów, które wystąpiły wspólnie w ramach pojedynczej sesji co najmniej 100 razy
- dla znalezionych grup przedmiotów należy obliczyć ufność dla wszystkich możliwych do utworzenia reguł asocjacyjnych,
np. X⇒Y i Y⇒X oraz (X,Y)⇒Z, (X,Z)⇒Y i (Y,Z)⇒X

Ważne! Reguły powinny zostać posortowane malejąco według ufności. W przypadku uzyskania tych samych wartości należy zastosować porządek leksykograficzny według poprzedników.

Plik wejściowy
- każda linia w pliku reprezentuje pojedynczą sesję użytkownika, a rozdzielone spacjami ośmioznakowe sygnatury to identyfikatory przeglądanych produktów

Ważne! Algorytm należy zaimplementować samodzielnie. Nie wolno korzystać z gotowych rozwiązań.

2. Przedstaw po 5 reguł asocjacyjnych o największej ufności dla par i trójek produktów.

Podpowiedzi
- istnieje 647 jednoelementowych zbiorów, które występują co najmniej 100 razy
- każda z 5 oczekiwanych reguł asocjacyjnych dla par produktów ma ufność większą niż 0.985