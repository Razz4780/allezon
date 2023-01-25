# allezon

## Komponenty
1. NGINX - przyjmuje requesty i rozdziela między ApiServery.
2. ApiServer - przyjmuje requesty. Wrzuca akcje użytkowników na Kafkę, czyta agregaty z baz danych.
3. Kafka - pośredniczy w konsumowaniu akcji użytkowników.
4. Consumer - czyta akcje użytkowników z Kafki i robi update'y baz danych.
5. ??? - baza danych do przechowywania profili użytkowników. Chyba wystarczy jakikolwiek key-value store, bo nie trzeba na tym robić żadnych query poza wyciąganiem wartości spod klucza. MongoDB może w [jednym requeście](https://stackoverflow.com/questions/29932723/how-to-limit-an-array-size-in-mongodb) dodać zdarzenie i przyciąć listę do ostatnich 200 elementów. Moglibyśmy trzymać dwie listy dla każdego użytkownika: `BUY` i `VIEW`.
6. ??? - baza danych do przechowywania agregatów. Tutaj to nie mam pojęcia :) Typ mówił, że Aerospike jest spoko + widziałem, że jest klient dla Rusta.

## Deployment
W życiu niczego nie deployowałem :). Komponenty trzeba skalować horyzontalnie, więc raczej trzeba użyć Kubernetesa i dać mu auto-scaling. Jeżeli chodzi o automated deployment, to można w GitHub Actions budować kontenery dockerowe i jakoś pushować je do chmurki RTB. Trzeba by wrzucić nasze hasła z RTB do sekretów repo.