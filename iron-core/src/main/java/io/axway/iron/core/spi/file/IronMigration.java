package io.axway.iron.core.spi.file;

import java.io.*;
import java.nio.file.FileVisitResult;
import java.nio.file.FileVisitor;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributes;
import com.google.common.base.Throwables;

/**
 * Migration script to migrate iron snapshots files from 0.5.0 layout to 0.6.0 layout
 */
public class IronMigration {
    public static void main(String[] args) {
        try {
            if (args.length != 4) {
                throw new IllegalArgumentException("Missing arguments");
            }

            Path ironPath = Paths.get(args[0]);
            if (!ironPath.toFile().exists() || !ironPath.toFile().isDirectory()) {
                throw new IllegalArgumentException("Unknown iron directory : " + args[0]);
            }

            String globalStoreManagerName = args[1];
            String storesStoreManagerName = args[2];
            if (globalStoreManagerName.length() == 0 || storesStoreManagerName.length() == 0) {
                throw new IllegalArgumentException("globalStoreManagerName and storesStoreManagerName must not be empty");
            }

            Path globalPath = ironPath.resolve("global").resolve("snapshot");
            Path targetGlobalPath = Paths.get(args[3]).resolve(globalStoreManagerName);
            Files.walk(globalPath)                                                    //
                    .map(Path::toFile)                                                //
                    .filter(File::isFile)                                             //
                    .filter(file -> file.getName().endsWith(".snapshot"))             //
                    .forEach(file -> {                                                //
                        String tx = file.getName().substring(0, 20);
                        try {
                            Path snapshotDir = targetGlobalPath.resolve("snapshot").resolve(tx);
                            snapshotDir.toFile().mkdirs();
                            Files.copy(file.toPath(), snapshotDir.resolve("global.snapshot"));
                        } catch (IOException e) {
                            throw Throwables.propagate(e);
                        }
                    });

            Path targetStoresPath = Paths.get(args[3]).resolve(storesStoreManagerName);
            Files.walkFileTree(ironPath, new FileVisitor<Path>() {
                @Override
                public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) {
                    String name = dir.getFileName().toString();
                    if(name.equals("global") || name.equals(".tmp") || name.equals("tx")) return FileVisitResult.SKIP_SUBTREE;
                    return FileVisitResult.CONTINUE;
                }

                @Override
                public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                    String store = file.getName(file.getNameCount() - 3).toString();
                    String tx = file.getFileName().toString().substring(0,20);
                    Path snapshotDir = targetStoresPath.resolve("snapshot").resolve(tx);
                    snapshotDir.toFile().mkdirs();
                    Files.copy(file, snapshotDir.resolve(store + ".snapshot"));
                    return FileVisitResult.CONTINUE;
                }

                @Override
                public FileVisitResult visitFileFailed(Path file, IOException exc) {
                    throw new RuntimeException(exc);
                }

                @Override
                public FileVisitResult postVisitDirectory(Path dir, IOException exc) {
                    return FileVisitResult.CONTINUE;
                }
            });

            System.out.println("Migration done to " + args[3]);
        } catch (Exception e) {
            System.err.println(
                    "Usage of migration tool : java -jar migrationTool.jar uriToIronDirectory globalStoreManagerName storesStoreManagerName uriToTargetDirectory\n"
                            + "\tglobalStoreManagerName and storesStoreManagerName must correspond to the names given in the code of traceability-ui");
            throw Throwables.propagate(e);
        }
    }
}
