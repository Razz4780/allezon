# allezon

## Komponenty
### Opcja 1
1. NGINX - przyjmuje requesty i rozdziela między ApiServery.
2. ApiServer - przyjmuje requesty. Wrzuca tagi na Kafkę, czyta agregaty i profile z Aerospike'a.
3. Kafka - pośredniczy w konsumowaniu tagów.
4. Consumer - czyta tagi z Kafki i pisze do Aerospike'a.
5. Aerospike - przechowuje wszystkie dane. Dane trzymamy w czterech setach: `buy-profiles`, `view-profiles`, `buy-aggregates` i `view-aggregates`. Sety dla profili trzymają rekordy o dwóch binach: `cookie_id` i `user_tags`, gdzie `user_tags` to mapa z timestampu w tagi. Indeks na `cookie_id` pozwoli na szybkie wyciągnięcie danych. Sety dla agregatów trzymają rekordy o binach: `timestamp` (przycięty do pełnej minuty), `origin`, `brand_id`, `category_id`, `count`, `sum_price`. Kombinacji `origin`, `brand_id` i `category_id` jest 16750000, ale limit na liczbę tagów w trakcie minuty wynosi 60000. Request `aggregates` ma timeout 60s. Czy wystarczający będzie indeks po `timestamp`?
6. GarbageCollector - nie musimy przechowywać danych starszych niż 24h, ale licząc od timestampu z ostatniego odebranego taga. To sprawia, że chyba nie możemy ustawić TTL na rekordach w Aerospike'u. GarbageCollector czyta tagi z Kafki i co jakiś czas wywala stare rekordy z `buy-aggregates` i `view-aggregates`.

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
