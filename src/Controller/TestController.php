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
