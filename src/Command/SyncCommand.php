<?php


namespace App\Command;

use Doctrine\DBAL\Connection;
use App\Entity\SynchronisationInfo;
use Doctrine\ORM\EntityManagerInterface;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Output\OutputInterface;
use Symfony\Bundle\FrameworkBundle\Console\Application;

class SyncCommand extends Command
{
    protected static $defaultName = 'run:sync';
    private EntityManagerInterface $entityManager;

    public function __construct(EntityManagerInterface $entityManager)
    {
        parent::__construct();
        $this->entityManager = $entityManager; // Initialize the entity manager
    }

    protected function configure(): void
    {
        $this->setDescription('Synchronizes data from MySQL to SQL Server.');
    }
    private function initializeSynchronization(): SynchronisationInfo
    {
        $synchronisation = new SynchronisationInfo();
        $synchronisation->setDateStart(new \DateTime());
        $this->entityManager->persist($synchronisation);
        $this->entityManager->flush();
        return $synchronisation;
    }
    private function initializeConnections(): void
    {
        $doctrine = $this->getApplication()->getKernel()->getContainer()->get('doctrine');
        $defaultEntityManager = $doctrine->getManager();
        $ugouvEntityManager = $doctrine->getManager('ugouv');

        $this->sqlServerConnection = $defaultEntityManager->getConnection();
        $this->mysqlConnection = $ugouvEntityManager->getConnection();
    }
    private function extractPrimaryKeyValues(array $data, array $primaryKey): array
    {
        $columnIds = [];

        foreach ($data as $row) {
            $keyValues = [];

            foreach ($primaryKey as $keyColumn) {
                // Determine if it's a composite key (array format) or single key (string)
                $columnName = is_array($keyColumn) && isset($keyColumn['ColumnName']) ? $keyColumn['ColumnName'] : $keyColumn;
                $keyValues[] = $row[$columnName] ?? null;
            }

            // Join composite key values with a dash; for single keys, it just contains one value
            $columnIds[] = implode('-', $keyValues);
        }

        return $columnIds;
    }

    /**
     * Handles synchronization errors by logging and updating the SynchronisationInfo record.
     */
    private function handleSynchronizationError(SynchronisationInfo $synchronisation, string $tableName, \Exception $e): void
    {
        $status = 'error in Table ' . ($tableName ?: 'N/A');
        $message = $e->getMessage() . ' in file ' . $e->getFile() . ' on line ' . $e->getLine();

        // Use the existing updateSyncInfo method
        $this->updateSyncInfo($synchronisation, $status, $message);
    }
    private function getAllTableNames(): array
    {
        $sql = "SELECT TABLE_NAME FROM information_schema.tables
                WHERE TABLE_TYPE = 'BASE TABLE'
                AND TABLE_SCHEMA = 'ugouv'
                AND TABLE_NAME NOT IN ('user_created_id','_biomed','_biomed_14_09_22','_biomed_15_09_22_mod','umouvement_antenne_')";

        $stmt = $this->sqlServerConnection->prepare($sql);
        return $stmt->executeQuery()->fetchAllAssociative();
//        dd($stmt->executeQuery()->fetchAllAssociative());
    }
//execute function
//    protected function execute(InputInterface $input, OutputInterface $output): int
//    {
//        set_time_limit(0); // 0 means no limit
//        ini_set('memory_limit', '-1'); // '-1' means unlimited memory
//
//        $tableName = '';
//        try {
//            $synchronisation = new SynchronisationInfo();
//            $synchronisation->setDateStart(new \DateTime());
//            $this->entityManager->persist($synchronisation);
//            $this->entityManager->flush();
//
//            $output->writeln('Starting data synchronization...');
//
//            // Get the default and 'ugouv' entity managers
//            $doctrine = $this->getApplication()->getKernel()->getContainer()->get('doctrine');
//            $defaultEntityManager = $doctrine->getManager();
//            $ugouvEntityManager = $doctrine->getManager('ugouv');
//
//            // Retrieve DBAL connections
//            $this->sqlServerConnection = $defaultEntityManager->getConnection();
//            $this->mysqlConnection = $ugouvEntityManager->getConnection();
//
//            $tables = $this->getAllTableNames();
////            dd($tables);
//            $tableCount = 1;
//            foreach ($tables as $table) {
//                $tableName = $table['TABLE_NAME'];
//                $tableName = '_biomed_14_09_22_(2)';
//
//                $output->writeln("$tableCount Processing table: $tableName");
//
//                $moreData = true;
//                $limit = 5000;
//                $tableCount++;
//
//                while ($moreData) {
//                    $data = $this->fetchUnsynchronizedData($tableName, $limit);
//                    if (!empty($data)) {
//                        $primaryKey = $this->getIdOrPrimaryKey($tableName);
////                            dd($primaryKey);
//                        if (!$primaryKey) {
//                            throw new \Exception("Table $tableName does not contain an 'id' column or primary key.");
//                        }
//                        // Check if we are dealing with a composite primary key
//                        if (is_array($primaryKey)) {
//                            // Composite key case: build an array of composite key values
//                            $columnIds = [];
//                            foreach ($data as $row) {
//                                $compositeKey = [];
//
//                                foreach ($primaryKey as $keyColumn) {
//                                    // Check if $keyColumn is an array or a string
//                                    if (is_array($keyColumn) && isset($keyColumn['ColumnName'])) {
//                                        // If $keyColumn is an array, use the 'ColumnName' key
//                                        $compositeKey[] = $row[$keyColumn['ColumnName']];
//                                    } else {
//                                        // If $keyColumn is a string, use it directly
//                                        $compositeKey[] = $row[$keyColumn];
//                                    }
//                                }
//
//                                // Concatenate composite key values with a separator (e.g., a dash or comma)
//                                $columnIds[] = implode('-', $compositeKey);
//                            }
//
//
//                        } else {
//                            // Single primary key case
//                            $columnIds = array_column($data, $primaryKey);
//                        }
//
//
//                        $this->upsertDataIntoTable($data, $tableName, $primaryKey);
//                        $this->flagDataAsSynchronized($tableName, $primaryKey, $data);
//                    } else {
//                        $moreData = false;
//                    }
//                }
//                gc_collect_cycles();
//            }
//
//            $output->writeln('Data synchronized successfully!');
//            $this->updateSyncInfo($synchronisation, 'success', 'Synchronization completed successfully.');
//            return Command::SUCCESS;
//
//        } catch (\Exception $e) {
//            $output->writeln('Error with table ' . ($tableName ?: 'N/A') . ': ' . $e->getMessage());
//
//            // Log the error and store it in the synchronization record
//            $this->updateSyncInfo(
//                $synchronisation,
//                'error in Table ' . ($tableName ?: 'N/A'),
//                $e->getMessage() . ' in file ' . $e->getFile() . ' on line ' . $e->getLine()
//            );
//            return Command::FAILURE;
//        }
//    }
    protected function execute(InputInterface $input, OutputInterface $output): int
    {
        // Set unlimited execution time and memory limit
        set_time_limit(0);
        ini_set('memory_limit', '-1');

        $tableName = '';
        try {
            $synchronisation = $this->initializeSynchronization();

            $output->writeln('Starting data synchronization...');
            $this->initializeConnections();

            $tables = $this->getAllTableNames();
//            dd($tables);
            $tableCount = 1;

            foreach ($tables as $table) {
                $tableName = $table['TABLE_NAME'];
//                $tableName='us_modules_dossiers';

                $output->writeln("$tableCount Processing table: $tableName");
                $tableCount++;
                $moreData = true;
                $limit = 20000;

                while ($moreData) {

                    $data = $this->fetchUnsynchronizedData($tableName, $limit);
                    if (!empty($data)) {
                        $primaryKey = $this->getIdOrPrimaryKey($tableName);

                        if (!$primaryKey) {
                            throw new \Exception("Table $tableName does not contain an 'id' column or primary key.");
                        }

                        $columnIds = $this->extractPrimaryKeyValues($data, $primaryKey);
//                        dd($columnIds);
                        $this->upsertDataIntoTable($data, $tableName, $primaryKey);
                        $this->flagDataAsSynchronized($tableName, $primaryKey, $data);
                    } else {
                        $moreData = false;
                    }
                }
                gc_collect_cycles();
            }

            $output->writeln('Data synchronized successfully!');
            $this->updateSyncInfo($synchronisation, 'success', 'Synchronization completed successfully.');
            return Command::SUCCESS;

        } catch (\Exception $e) {
            $output->writeln('Error with table ' . ($tableName ?: 'N/A') . ': ' . $e->getMessage());
            $this->handleSynchronizationError($synchronisation, $tableName, $e);
            return Command::FAILURE;
        }
    }


    private function fetchUnsynchronizedData(string $tableName, int $limit): array
    {
        $sql = "SELECT * FROM $tableName WHERE flag_synchronisation_locale = 0 OR flag_synchronisation_locale IS NULL LIMIT $limit";
        $stmt = $this->mysqlConnection->prepare($sql);
        return $stmt->executeQuery()->fetchAllAssociative();
    }

    private function flagDataAsSynchronized(string $tableName, array $primaryKey, array $data): void
    {
        if (empty($data)) {
            return;
        }

        $whereClauses = [];
        foreach ($data as $row) {
            $conditions = [];
            if (isset($primaryKey[0]['ColumnName'])) {
                foreach ($primaryKey as $keyColumn) {
                    $columnName = $keyColumn['ColumnName'];
                    $conditions[] = "$columnName = " . $this->mysqlConnection->quote($row[$columnName]);
                }
            } else {
                $primaryKeyColumn = $primaryKey[0];
                $conditions[] = "$primaryKeyColumn = " . $this->mysqlConnection->quote($row[$primaryKeyColumn]);
            }
            $whereClauses[] = '(' . implode(' AND ', $conditions) . ')';
        }

        $whereClause = implode(' OR ', $whereClauses);

        $updateSql = "UPDATE $tableName SET flag_synchronisation_locale = 1 WHERE $whereClause";
        $this->mysqlConnection->executeQuery($updateSql);
    }

    private function upsertDataIntoTable(array $data, string $tableName, array $primaryKey): void
    {
        $tableNameSchema = "ugouv" . '.' . $tableName;
        $this->sqlServerConnection->beginTransaction();

        try {
            $this->sqlServerConnection->executeQuery('ALTER TABLE ' . $tableNameSchema . ' NOCHECK CONSTRAINT ALL');
            $columnTypes = $this->getColumnTypes($tableName, 'ugouv');

            foreach ($data as $row) {
                foreach ($row as $column => $value) {
                    if (in_array($columnTypes[$column], ['datetime', 'date', 'datetime2'])) {
                        if (!$this->validateDate($value)) {
                            $row[$column] = null;
                        }
                    }
                }

                $whereConditions = [];
                if (isset($primaryKey[0]['ColumnName'])) {
                    foreach ($primaryKey as $keyColumn) {
                        $columnName = $keyColumn['ColumnName'];
                        $whereConditions[] = "$columnName = " . $this->sqlServerConnection->quote($row[$columnName]);
                    }
                } else {
                    $primaryKeyColumn = $primaryKey[0];
                    $whereConditions[] = "$primaryKeyColumn = " . $this->sqlServerConnection->quote($row[$primaryKeyColumn]);
                }

                $whereClause = implode(' AND ', $whereConditions);
                $existsQuery = $this->sqlServerConnection->createQueryBuilder()
                    ->select('*')
                    ->from($tableNameSchema)
                    ->where($whereClause)
                    ->executeQuery()
                    ->fetchOne();

                if ($existsQuery) {
                    $this->updateRecord($tableNameSchema, $row, $primaryKey);
                } else {
                    $this->insertRecord($tableNameSchema, $row);
                }
            }

            $this->sqlServerConnection->executeQuery('ALTER TABLE ' . $tableNameSchema . ' CHECK CONSTRAINT ALL');
            $this->sqlServerConnection->commit();

        } catch (\Exception $e) {
            $this->sqlServerConnection->rollBack();
            throw $e;
        }
    }

    private function getIdOrPrimaryKey(string $tableName): ?array
    {
        $idCondition = "(COLUMN_NAME = 'id' OR COLUMN_NAME = 'ID')";
        $sql = "SELECT COLUMN_NAME
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_NAME = '$tableName'
            AND $idCondition
            AND TABLE_SCHEMA = 'ugouv';";

        $stmt = $this->sqlServerConnection->prepare($sql);
        $result = $stmt->executeQuery();
        $idColumn = $result->fetchOne();

        if ($idColumn) {
            return [$idColumn];
        }

        $primaryKeys = $this->getPrimaryKeys($tableName);
        return count($primaryKeys) > 0 ? $primaryKeys : null;
    }

    private function getPrimaryKeys(string $tableName): array
    {
        $sql = "
        SELECT 
            c.name AS ColumnName
        FROM 
            sys.indexes i
            INNER JOIN sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id
            INNER JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
            INNER JOIN sys.tables t ON i.object_id = t.object_id
        WHERE 
            i.is_primary_key = 1
            AND t.name = '$tableName'";

        $stmt = $this->sqlServerConnection->prepare($sql);
        $result = $stmt->executeQuery();
        return $result->fetchAllAssociative();
    }

    private function validateDate($date, $format = 'Y-m-d H:i:s'): bool
    {
        if ($date === null || $date === '' || $date === '0000-00-00' || $date === '0000-00-00 00:00:00') {
            return false;
        }

        $d = \DateTime::createFromFormat($format, $date);
        return $d && $d->format($format) === $date;
    }

    private function getColumnTypes(string $tableName, string $schema): array
    {
        $sql = "
            SELECT COLUMN_NAME, DATA_TYPE
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_NAME = ?
            AND TABLE_SCHEMA = ?";

        $stmt = $this->sqlServerConnection->prepare($sql);
        $stmt->bindValue(1, $tableName);
        $stmt->bindValue(2, $schema);

        return $stmt->executeQuery()->fetchAllKeyValue();
    }

    private function updateRecord(string $tableNameSchema, array $row, array $primaryKey): void
    {
        $qb = $this->sqlServerConnection->createQueryBuilder();
        $qb->update($tableNameSchema);

        foreach ($row as $column => $value) {
            if (!in_array($column, array_column($primaryKey, 'ColumnName')) && !in_array($column, $primaryKey)) {
                $qb->set($column, '?');
                $qb->setParameter(count($qb->getParameters()), $value);
            }
        }

        if (isset($primaryKey[0]['ColumnName'])) {
            $conditions = [];
            foreach ($primaryKey as $keyColumn) {
                $columnName = $keyColumn['ColumnName'];
                $conditions[] = "$columnName = ?";
                $qb->setParameter(count($qb->getParameters()), $row[$columnName]);
            }
            $qb->where(implode(' AND ', $conditions));
        } else {
            $primaryKeyColumn = $primaryKey[0];
            $qb->where("$primaryKeyColumn = ?");
            $qb->setParameter(count($qb->getParameters()), $row[$primaryKeyColumn]);
        }

        $qb->executeStatement();
    }

    private function insertRecord(string $tableNameSchema, array $row): void
    {
        $qb = $this->sqlServerConnection->createQueryBuilder();
        $qb->insert($tableNameSchema);

        foreach ($row as $column => $value) {
            if ($column != 'user' && $column != 'public') {
                $qb->setValue($column, '?');
            } else {
                $qb->setValue('[' . $column . ']', '?');
            }
            $qb->setParameter(count($qb->getParameters()), $value);
        }
        $qb->executeStatement();
    }

    private function updateSyncInfo(SynchronisationInfo $synchronisation, string $status, string $message): void
    {
        $synchronisation->setInfo($status);
        $synchronisation->setDateEnd(new \DateTime());
        $synchronisation->setMessage($message);
        $this->entityManager->flush();
    }
}
