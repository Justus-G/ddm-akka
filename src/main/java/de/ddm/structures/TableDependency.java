package de.ddm.structures;

import de.ddm.serialization.AkkaSerializable;
import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public class TableDependency implements AkkaSerializable {
    int tableID1;
    int tableID2;
    int colID1;
    int colID2;
}
