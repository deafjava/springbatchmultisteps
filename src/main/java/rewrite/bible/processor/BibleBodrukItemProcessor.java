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

    private Integer bibleVersion;

    @Override
    public Bible process(BibleBodruk item) {

        final Integer book = Integer.valueOf(item.getBook());
        final String citation = item.getText();
        final Integer chapter = Integer.valueOf(item.getChapter());
        final Integer verse = Integer.valueOf(item.getVerse());
        final Integer version = bibleVersion;

        final Bible bible = new Bible(book, citation, chapter, verse, version);

        if (chapter == 1 && verse == 1) {
            log.info("Writing book seq: #" + book + "...");
        }
        return bible;
    }
}
