/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package de.marx_software.webtools.core.modules.analytics.db.index.lucene.shard.migration;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Stream;
import static java.nio.file.StandardCopyOption.*;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

/**
 *
 * @author marx
 */
public class Backup {
	
	final static String europeanDatePattern = "dd.MM.yyyy.HH.mm";
	final static DateTimeFormatter europeanDateFormatter = DateTimeFormatter.ofPattern(europeanDatePattern);
	
	public String extension () {
		return europeanDateFormatter.format(LocalDateTime.now());
	}

	public void backup(Path src, Path dest) throws IOException {
		try (Stream<Path> stream = Files.walk(src)) {
			stream.forEach(source -> copy(source, dest.resolve(src.relativize(source))));
		}
	}

	private void copy(Path source, Path dest) {
		try {
			Files.copy(source, dest, REPLACE_EXISTING);
		} catch (Exception e) {
			throw new RuntimeException(e.getMessage(), e);
		}
	}
	
	public static void main (String... args) throws IOException {
		Backup backup = new Backup();
		
		backup.backup(
				Path.of("../../temp/analytics/index/shard_0000000001/index"), 
				Path.of("../../temp/analytics/index/shard_0000000001/index." + backup.extension()));
	}
}
