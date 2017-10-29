package rewrite.bible.processor;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ItemProcessor;
import rewrite.bible.dto.Bible;
import rewrite.bible.dto.BibleSource;

public class BibleItemASVProcessor implements ItemProcessor<BibleSource, Bible> {

    private static final Logger log = LoggerFactory.getLogger(BibleItemASVProcessor.class);

    @Override
    public Bible process(BibleSource item) throws Exception {

        final Integer book = Integer.valueOf(item.getBook());
        final String citation = item.getCitation();
        final Integer chapter = Integer.valueOf(item.getChapter());
        final Integer verse = Integer.valueOf(item.getChapter());
        final String version = "asv";

        final Bible bible = new Bible(book, citation, chapter, verse, version);

        log.info("Version - " + version + " - Book seq: #" + book + ", chap: #" + chapter + ", verse: #" + verse);

        return bible;
    }
}
