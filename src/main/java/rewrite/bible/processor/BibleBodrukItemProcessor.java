package rewrite.bible.processor;


import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.item.ItemProcessor;
import rewrite.bible.dto.Bible;
import rewrite.bible.dto.BibleBodruk;
import rewrite.bible.dto.BibleSource;

@Slf4j
@AllArgsConstructor
public class BibleBodrukItemProcessor implements ItemProcessor<BibleBodruk, Bible> {

    private String bibleVersion;

    @Override
    public Bible process(BibleBodruk item) {

        final Integer book = Integer.valueOf(item.getBook());
        final String citation = item.getText();
        final Integer chapter = Integer.valueOf(item.getChapter());
        final Integer verse = Integer.valueOf(item.getVerse());
        final String version = bibleVersion;

        final Bible bible = new Bible(book, citation, chapter, verse, version);

        log.info("Version - " + version + " - Book seq: #" + book + ", chap: #" + chapter + ", verse: #" + verse);

        return bible;
    }
}
