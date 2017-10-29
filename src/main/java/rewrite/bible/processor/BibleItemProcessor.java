package rewrite.bible.processor;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ItemProcessor;
import rewrite.bible.dto.Bible;
import rewrite.bible.dto.BibleSource;

public class BibleItemProcessor implements ItemProcessor<BibleSource, Bible> {

    private static final Logger log = LoggerFactory.getLogger(BibleItemProcessor.class);

    private String bibleVersion;

    public BibleItemProcessor(String bibleVersion) {

        this.bibleVersion = bibleVersion;
    }

    @Override
    public Bible process(BibleSource item) throws Exception {

        final Integer book = Integer.valueOf(item.getBook());
        final String citation = item.getCitation();
        final Integer chapter = Integer.valueOf(item.getChapter());
        final Integer verse = Integer.valueOf(item.getVerse());
        final String version = bibleVersion;

        final Bible bible = new Bible(book, citation, chapter, verse, version);

        log.info("Version - " + version + " - Book seq: #" + book + ", chap: #" + chapter + ", verse: #" + verse);

        return bible;
    }
}
