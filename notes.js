// client

var localRepository = LocalRepository.create(...);

localRepository.use(LocalHistory.create());

var remoteRepository = RemoteRepository.create(...);

remoteRepository.use(RemoteHistory.create());

var synchronizer = RepositorySynchronizer.create(localRepository, remoteRepository);

// server

var repository = LocalRepository(...);

repository.use(LocalHistory.create({ projection: ['tenantId'] }));

var repositoryServer = RepositoryServer.create(repository, repository);

repositoryServer.use(HistoryServer.create());

// algorythme

/*
* Le client mobile lit le remoteHistory depuis le dernier numéro reçu. Seules les entrées ayant pour origine un repository différent du localRepository sont prises en compte. À partir des entrées reçues, on peut lire toutes les données sur le remoteRepository et les appliquées localement. Ces nouvelles données ayant pour origine un repository différent du localRepository n'ajouteront pas de nouvelles entrées dans le localHistory, au contraire, elles supprimeront les entrées en conflict.

* Le client mobile consulte le localHistory pour déterminer les données à envoyer au remoteRepository. Quand toutes les données ont été envoyées, le localHistory est effacé.
*/

// API

/*
GET /v1/history
Response: {
  repositoryId: 'sdg87dsg8',
  lastSequenceNumber: 4530
}

GET /v1/history-items?startAfterSequenceNumber=3993&filter.tenantId=ccc&ignoreOriginRepositoryId=jh56HJ3
Response: {
  repositoryId: 'sdg87dsg8',
  lastSequenceNumber: 4530,
  items: [
    {
      primaryKey: 'kjdhs786dsgk',
      isDeleted: true,
      originRepositoryId: 'jh56HJ3aa09x'
    },
    ...
  ]
}

POST /v1/participants/get-items
Request: ['kjdhs786dsgk', ...]
*/

// données

/*
$Repository (singleton)
  id
  lastHistorySequenceNumber
  remoteRepositoryId
  remoteHistoryLastSequenceNumber

$History
  primaryKey
  isDeleted
  sequenceNumber
  originRepositoryId
*/

['$History:primaryKey', 'kjdhs786dsgk'] => { sequenceNumber: 4530 }

['$History:sequenceNumber', 4530] => { primaryKey: 'kjdhs786dsgk', isDeleted: true, originRepositoryId: 'jh56HJ3aa09x' }
