package rewrite.bible.dto;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@AllArgsConstructor
@ToString
public class Bible {

    private Integer book;
    private String citation;
    private Integer chapter;
    private Integer verse;
    private Integer version;
}
