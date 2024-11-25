<?php

namespace App\Controller;

use Doctrine\DBAL\Connection;
use Doctrine\Persistence\ManagerRegistry;
use Symfony\Component\HttpFoundation\JsonResponse;
use Symfony\Component\Routing\Annotation\Route;
use Symfony\Bundle\FrameworkBundle\Controller\AbstractController;

class TestController extends AbstractController
{
    private $connection;

    public function __construct(Connection $connection)
    {
        $this->connection = $connection;
    }
    #[Route('/testmysql', name: 'testmysql')]
    public function testmysql(ManagerRegistry $doctrine): JsonResponse
    {
        // Use the MySQL connection from the 'ugouv' entity manager
        $mysqlConnection = $doctrine->getManager('ugouv')->getConnection();

        // // Execute a query
        $testData = $mysqlConnection->fetchAllAssociative("SELECT * FROM avance");

        // Output the data
        return new JsonResponse($testData);
    }
    #[Route('/test/tables', name: 'test_tables', methods: ['GET'])]
    public function getAllTableNames(): JsonResponse
    {
        // SQL query to fetch all base table names excluding 'synchronisation_info'
        $sql = "SELECT TABLE_NAME 
                FROM information_schema.tables
                WHERE TABLE_TYPE = 'BASE TABLE'
                  AND TABLE_SCHEMA = 'ugouv'
                  AND TABLE_NAME IN (
                      't_achatdemandeinternecab', 
                      'ua_t_commandefrscab', 
                      'ua_t_livraisonfrscab', 
                      'ua_t_facturefrscab', 
                      'uv_deviscab', 
                      'uv_commandecab', 
                      'uv_livraisoncab', 
                      'uv_facturecab', 
                      'p_dossier', 
                      'u_p_partenaire', 
                      'p_partenaire_categorie', 
                      'u_general_operation', 
                      'tr_transaction'
                  );";

        // Prepare and execute the query
        $stmt = $this->connection->prepare($sql);
        $result = $stmt->executeQuery();

        // Fetch all table names
        $tableNames = $result->fetchAllAssociative();

        // Return table names as JSON for easy viewing
        return new JsonResponse($tableNames);
    }
}
    // Method to handle inserting or updating data in the local database
    // private function upsertDataIntoTable(array $data, string $tableName): void
    // {
    //     $this->connection->beginTransaction();

    //     try {
    //         $this->connection->executeQuery('SET foreign_key_checks = 0');

    //         foreach ($data as $row) {
    //             $columns = array_keys($row);

    //             $sql = sprintf(
    //                 'INSERT INTO %s (%s) VALUES (%s) ON DUPLICATE KEY UPDATE %s',
    //                 $tableName,
    //                 implode(',', $columns),
    //                 implode(',', array_map(fn($col) => ':' . $col, $columns)),
    //                 implode(',', array_map(fn($col) => "$col = VALUES($col)", $columns))
    //             );

    //             $stmt = $this->connection->prepare($sql);

    //             foreach ($row as $column => $value) {
    //                 $stmt->bindValue(':' . $column, $value);
    //             }

    //             $stmt->executeQuery();
    //         }

    //         $this->connection->commit();
    //         $this->connection->executeQuery('SET foreign_key_checks = 1');
    //     } catch (\Exception $e) {
    //         $this->connection->rollBack();
    //         $this->connection->executeQuery('SET foreign_key_checks = 1');
    //         throw $e;
    //     }
    // }


    
// namespace App\Command;

// use Doctrine\DBAL\Connection;
// use Symfony\Component\Console\Command\Command;
// use Symfony\Component\Console\Input\InputInterface;
// use Symfony\Component\Console\Output\OutputInterface;
// use Symfony\Component\Console\Style\SymfonyStyle;

// class InsertExampleCommand extends Command
// {
//     protected static $defaultName = 'app:insert-example';
//     private $connection;

//     // Inject the Doctrine DBAL connection
//     public function __construct(Connection $connection)
//     {
//         parent::__construct();
//         $this->connection = $connection;
//     }

//     protected function configure(): void
//     {
//         // Set a description for the command
//         $this->setDescription('Inserts a new record into Tbl_Example using Doctrine QueryBuilder in SQL Server');
//     }

//     protected function execute(InputInterface $input, OutputInterface $output): int
//     {
//         $io = new SymfonyStyle($input, $output);

//         try {
//             // Create the QueryBuilder instance
//             $qb = $this->connection->createQueryBuilder();

//             // Build the insert query
//             $qb->insert('Tbl_Example')
//                ->values([
//                    'ID' => ':id',
//                    'Name' => ':name'
//                ])
//                ->setParameter('id', 2)
//                ->setParameter('name', 'Jane Smith');

//             // Execute the query
//             $qb->executeStatement();

//             $io->success('Inserted a new record into Tbl_Example using Doctrine QueryBuilder in SQL Server.');
//         } catch (\Exception $e) {
//             $io->error('Error: ' . $e->getMessage());
//             return Command::FAILURE;
//         }

//         return Command::SUCCESS;
//     }
// }


//     private function upsertDataIntoTable(array $data, string $tableName): void
//     {
//         $tableNameSchema = "ugouv" . '.' . $tableName;
//         $this->connection->beginTransaction();
//
//         try {
//             // Disable foreign key checks
//             $this->connection->executeQuery('ALTER TABLE ' . $tableNameSchema . ' NOCHECK CONSTRAINT ALL');
//
//             foreach ($data as $row) {
//                 $columns = array_keys($row);
//                 $primaryKey = 'id'; // Assuming 'id' is the primary key
//
//                 // Check if the record already exists
//                 $existsQuery = $this->connection->createQueryBuilder()
//                     ->select($primaryKey)
//                     ->from($tableNameSchema)
//                     ->where("$primaryKey = :id")
//                     ->setParameter('id', $row[$primaryKey])
//                     ->executeQuery()
//                     ->fetchOne();
//
//                 if ($existsQuery) {
//                     // Update the record if it exists
//                     $qb = $this->connection->createQueryBuilder();
//                     $qb->update($tableNameSchema);
//
//                     // Add columns and values for the update query
//                     foreach ($columns as $column) {
//                         if ($column !== $primaryKey) {
//                             $qb->set($column, ':' . $column);
//                             $qb->setParameter($column, $row[$column]);
//                         }
//                     }
//
//                     $qb->where("$primaryKey = :id")
//                        ->setParameter('id', $row[$primaryKey]);
//
//                     // Execute the update query
//                     $qb->executeStatement();
//                 } else {
//                     // Insert the record if it doesn't exist
//                     $qb = $this->connection->createQueryBuilder();
//                     $qb->insert($tableNameSchema);
//                     // Add columns and values for the insert query
//                     foreach ($columns as $column) {
//                         $qb->setValue($column, ':' . $column);
//                         $qb->setParameter($column, $row[$column]);
//                     }
//
//                     // Execute the insert query
//                     $qb->executeStatement();
//                 }
//             }
//
//             // Re-enable foreign key constraints
//             // $this->connection->executeQuery('ALTER TABLE ' . $tableNameSchema . ' WITH CHECK CHECK CONSTRAINT ALL');
//             $this->connection->commit();
//
//         } catch (\Exception $e) {
//             $this->connection->rollBack();
//             // $this->connection->executeQuery('ALTER TABLE ' . $tableNameSchema . ' WITH CHECK CHECK CONSTRAINT ALL');
//             throw $e; // Re-throw the exception
//         }
//     }