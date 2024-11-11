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

    protected function configure(): void
    {
        $this->setDescription('Synchronizes data from MySQL to SQL Server.');
    }

    protected function execute(InputInterface $input, OutputInterface $output): int
    {
        try {
            $output->writeln('Starting data synchronization...');

            // Get the default and 'ugouv' entity managers
            $doctrine = $this->getApplication()->getKernel()->getContainer()->get('doctrine');
            $defaultEntityManager = $doctrine->getManager();
            $ugouvEntityManager = $doctrine->getManager('ugouv');

            // Retrieve DBAL connections
            $this->sqlServerConnection = $defaultEntityManager->getConnection();
            $this->mysqlConnection = $ugouvEntityManager->getConnection();

            $tables = $this->getAllTableNames();
//            dd($tables);
            $tableCount = 1;
            foreach ($tables as $table) {
                $tableName = $table['TABLE_NAME'];
                $tableName = 'ua_t_facturefrsdet';

                $output->writeln("$tableCount Processing table: $tableName");

                $moreData = true;
                $limit = 200;
                $tableCount++;

                while ($moreData) {
                    $data = $this->fetchUnsynchronizedData($tableName, $limit);
                    if (!empty($data)) {
                        $primaryKey = $this->getIdOrPrimaryKey($tableName);
                        if (!$primaryKey) {
                            throw new \Exception("Table $tableName does not contain an 'id' column or primary key.");
                        }

                        $this->upsertDataIntoTable($data, $tableName, $primaryKey);
                        $this->flagDataAsSynchronized($tableName, $primaryKey, $data);
                    } else {
                        $moreData = false;
                    }
                }
            }

            $output->writeln('Data synchronized successfully!');
            return Command::SUCCESS;
        } catch (\Exception $e) {
            $output->writeln('An error occurred: ' . $e->getMessage());
            return Command::FAILURE;
        }
    }

    private function getAllTableNames(): array
    {
        $sql = "SELECT TABLE_NAME FROM information_schema.tables
                WHERE TABLE_TYPE = 'BASE TABLE'
                AND TABLE_SCHEMA = 'ugouv'
                AND TABLE_NAME NOT IN ('user_created_id','_biomed','_biomed_14_09_22','_biomed_14_09_22_(2)','_biomed_15_09_22_mod','umouvement_antenne_' )";

        $stmt = $this->sqlServerConnection->prepare($sql);
        return $stmt->executeQuery()->fetchAllAssociative();
//        dd($stmt->executeQuery()->fetchAllAssociative());
    }

    private function fetchUnsynchronizedData(string $tableName, int $limit): array
    {
        $sql = "SELECT * FROM $tableName WHERE flag_synchronisation_locale = 0 OR flag_synchronisation_locale IS NULL ";
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