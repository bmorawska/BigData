## Przetwarzanie i analiza dużych zbiorów danych

### Analiza opóźnień ruchu lotniczego w Stanach Zjednoczonych w latach 1995 - 2020

## Motywacja
Mimo tego, że lecący samolot nie może utknąć w korku jak samochód, ani nie musi stawać na czerwonym świetle, to podróże samolotem są w przekonaniu pasażerów jednymi z najbardziej narażonych na opóźnienia. Panuje powszechne przekonanie, że na lotnisko najlepiej należy przyjechać kilka godzin wcześniej, bo jest tyle rzeczy, które mogą się opóźnić i utrudnić bądź uniemożliwić podróż. Po opuszczeniu pokładu np. czekanie na walizki również wydłuża czas dotarcia do celu.

Warto zastanowić się nad charakterystyką opóźnień i sprawdzić, czy możliwe jest np. uniknięcie części z nich. A może zamiast spędzać cały dzień w podróży da się spędzić w niej tylko pół, podczas gdy drugie pół zostaje na wakacyjny relaks?

## Cele
Celem projektu jest zbadanie i graficzne podsumowanie wybranych cech zbioru danych dających odpowiedzi na część lub, jeśli to możliwe, na wszystkie poniższe pytania:
- Kiedy jest najlepsza pora dnia / dzień tygodnia / pora roku na lot, aby zminimalizować opóźnienia?
- Czy starsze samoloty mają większe opóźnienia?
- Jak zmienia się liczba osób podróżujących między różnymi lokalizacjami w czasie?
- Jak wpływa pogoda/prognoza pogody na opóźnienia samolotu?
- Czy możliwe jest znalezienie „wąskiego gardła” (ang. bottleneck), czyli miejsca, w którym opóźnienie na danym lotnisku wpływa znacząco na opóźnienia na innych lotniskach?
- Jaki wpływ na opóźnienia i częstotliwość lotów miał zamach terrorystyczny na World Trade Center w 2001 roku i pandemia koronawirusa w 2020 roku?

## Cel naukowy
- Poszukiwanie najbardziej efektywnej metody przeszukiwania grafu, w celu znalezienia lotniska-bottleneck. Ocena efektywności odbywać się będzie pod kątem skuteczności oraz czasu wykonania.
- Poszukiwanie cech, które najbardziej wpływają na opóźnienia samolotów.
…

## Zbiór danych
- Dane pochodzą ze strony Ministerstwa Transportu Stanów Zjednoczonych.
- Dane obejmują szczegóły przylotów i odlotów wszystkich lotów komercyjnych w USA, od stycznia 1995 r. do września 2020 r.
- Każdy lot jest opisany maksymalnie przez 110 wartości, które pogrupowane są na następujące kategorie:
podstawowe parametry (dzień, miesiąc, rok)
    - dane dotyczące linii lotniczych
    - dane dotyczące lotniska, z którego wyruszył samolot
    - dane dotyczące lotniska docelowego
    - dane dotyczące opóźnień przed lotem, wynikających z lotu i po przylocie
    - przesunięcia, przekierowania lub likwidację lotu
    - statystyk dotyczących lotu (czas lotu, pokonana odległość itp.)
Obecnie baza ma około 70 GB danych, a co miesiąc przybywa około 250 MB rekordów o 400 – 450 tysiącach lotów.

## Metody analizy danych
1. Redukcja liczby wymiarów, usunięcie danych niepotrzebnych i błędów.
2. Analiza statystyczna lotów 
3. Opisanie zależności pomiędzy cechami takimi jak: pogoda i opóźnienia, dzień tygodnia i opóźnienia itp. za pomocą np. krzywej regresji.  
4. Analiza grafowa połączeń, pozwalająca odnaleźć lotniska-bottleneck, które generują największe opóźnienia.

## Technologie i języki programowania
Python i Spark

