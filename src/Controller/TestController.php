<?php

namespace App\Controller;

use Doctrine\DBAL\Connection;
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

    #[Route('/test/tables', name: 'test_tables', methods: ['GET'])]
    public function getAllTableNames(): JsonResponse
    {
        // SQL query to fetch all base table names excluding 'synchronisation_info'
        $sql = "SELECT TABLE_NAME FROM information_schema.tables
                WHERE TABLE_TYPE = 'BASE TABLE'
                AND TABLE_NAME != 'synchronisation_info'";

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