package rewrite.bible.dto;

import lombok.Data;

@Data
public class BibleBodruk {
    private String id;
    private String version;
    private String testament;
    private String book;
    private String chapter;
    private String verse;
    private String text;
}
