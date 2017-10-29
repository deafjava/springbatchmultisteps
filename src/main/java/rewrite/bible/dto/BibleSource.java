package rewrite.bible.dto;

import lombok.Data;

@Data
public class BibleSource {

    // "01001001","1","1","1","In the beginning God created the heavens and the earth."
    private String personalizedId;
    private String book;
    private String chapter;
    private String verse;
    private String citation;

}
