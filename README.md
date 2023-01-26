# allezon

## Komponenty
### Opcja 1
1. NGINX - przyjmuje requesty i rozdziela między ApiServery.
2. ApiServer - przyjmuje requesty. Wrzuca tagi na Kafkę, czyta agregaty i progile z baz danych.
3. Kafka - pośredniczy w konsumowaniu tagów.
4. Consumer - czyta tagi z Kafki i robi update'y baz danych.
5. ??? - baza danych do przechowywania profili użytkowników. Chyba wystarczy jakikolwiek key-value store, bo nie trzeba na tym robić żadnych query poza wyciąganiem wartości spod klucza. MongoDB może w [jednym requeście](https://stackoverflow.com/questions/29932723/how-to-limit-an-array-size-in-mongodb) dodać zdarzenie i przyciąć listę do ostatnich 200 elementów. Moglibyśmy trzymać dwie listy dla każdego użytkownika: `BUY` i `VIEW`.
6. ??? - baza danych do przechowywania agregatów. Tutaj to nie mam pojęcia :) Typ mówił, że Aerospike jest spoko + widziałem, że jest klient dla Rusta.

### Opcja 2
1. NGINX - przyjmuje requesty i rozdziela między ApiServery.
2. ApiServer - przyjmuje requesty, wrzuca tagi na Kafkę, czyta agregaty i profile ze Scylli.
3. Kafka - pośredniczy w konsumowaniu tagów.
4. Consumer - czyta tagi z Kafki i wrzuca je na Scyllę.
5. ScyllaDB - trzyma wszystkie dane. Replication factor 2, read consistency 1, write consistency QUORUM.

Pytania:
1. Kiedy wygodnie robić truncate profili do ostatnich 200 tagów?
2. Jedna tabela na profile `BUY` i jedna na `VIEW`, jedna na wszystko, po jednej dla każdego `cookie_id`?
3. Jedna tabela na wszystkie agregaty czy jedna na każdą kombinację (`action` i `aggregates[]`)?
4. Driver Rusta do Scylli jest w becie i ma dużo issuesów, pisali to ludzie z MIM-u na licencjat. Raczej gówno. Czy można użyć drivera do Cassandry?

## Deployment
W życiu niczego nie deployowałem :). Komponenty trzeba skalować horyzontalnie, więc raczej trzeba użyć Kubernetesa i dać mu auto-scaling. Jeżeli chodzi o automated deployment, to można w GitHub Actions budować kontenery dockerowe i jakoś pushować je do chmurki RTB. Trzeba by wrzucić nasze hasła z RTB do sekretów repo.
