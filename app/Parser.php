<?php

namespace App;

final class Parser
{
    private const string DELIMITER = ',';
    private const int STICHER_PREFIX_LENGTH = 19;
    private const int DATE_LENGTH = 10;


    public function parse(string $inputPath, string $outputPath): void
    {
        $numProcesses = 4;
        $fileSize = filesize($inputPath);
        $chunkSize = ceil($fileSize / $numProcesses);
        $tempFiles = [];
        $pids = [];

        for ($i = 0; $i < $numProcesses; $i++) {
            $tempFiles[$i] = tempnam(sys_get_temp_dir(), "parser_chunk_$i");
            $startByte = $i * $chunkSize;
            $endByte = min(($i + 1) * $chunkSize - 1, $fileSize - 1);

            $pid = pcntl_fork();
            if ($pid == -1) {
                die("Could not fork process");
            } elseif ($pid == 0) {
                self::processChunk($inputPath, $startByte, $endByte, $tempFiles[$i]);
                exit(0);
            } else {
                $pids[] = $pid;
            }
        }

        foreach ($pids as $pid) {
            pcntl_waitpid($pid, $status);
        }

        $existingPath = [];
        for ($i = 0; $i < $numProcesses; $i++) {
            $chunkResults = unserialize(file_get_contents($tempFiles[$i]));
            unlink($tempFiles[$i]);

            foreach ($chunkResults as $path => $dates) {
                foreach ($dates as $date => $count) {
                    $existingPath[$path][$date] = ($existingPath[$path][$date] ?? 0) + $count;
                }
            }
        }

        foreach ($existingPath as $path => $dates) {
            ksort($existingPath[$path]);
        }

        file_put_contents($outputPath, json_encode($existingPath, JSON_PRETTY_PRINT));
    }

    public static function processChunk(string $inputPath, int $startByte, int $endByte, string $outputFile): void
    {
        $file = fopen($inputPath, 'r');
        $existingPath = [];

        fseek($file, $startByte);

        if ($startByte > 0) {
            fgets($file);
        }

        while (ftell($file) <= $endByte && ($line = fgets($file)) !== false) {
            $content = substr($line, self::STICHER_PREFIX_LENGTH);
            $commaPos = strpos($content, self::DELIMITER);
            if ($commaPos === false) {
                continue;
            }

            $path = substr($content, 0, $commaPos);

            $date = substr($content, $commaPos + 1, self::DATE_LENGTH);
            if (strlen($date) !== self::DATE_LENGTH) {
                continue;
            }

            $existingPath[$path][$date] = ($existingPath[$path][$date] ?? 0) + 1;
        }

        fclose($file);

        file_put_contents($outputFile, serialize($existingPath));
    }
}
