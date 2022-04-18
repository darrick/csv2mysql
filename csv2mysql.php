<?php

require __DIR__ . '/vendor/autoload.php';

use App\Command\ConvertCommand;
use App\Command\SchemaCommand;
use Symfony\Component\Console\Application;

$app = new Application();
$app->add(new ConvertCommand());
$app->add(new SchemaCommand());
$app->run();
