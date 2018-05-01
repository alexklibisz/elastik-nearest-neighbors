package org.elasticsearch.plugin.ann;

import java.util.ArrayList;
import java.util.List;

public class LshModel {

    private Integer nbTables;
    private Integer nbBitsPerTable;
    private Integer nbDimensions;
    private List<List<List<Float>>> normalVectors;
    private List<List<List<Float>>> midPoints;

    public LshModel(Integer nbTables, Integer nbBitsPerTable, Integer nbDimensions) {
        this.nbTables = nbTables;
        this.nbBitsPerTable = nbBitsPerTable;
        this.nbDimensions = nbDimensions;
    }

//    private class Hyperplane {
//        public Hyperplane()
//    }

    public void fitFromVectorSample(List<List<Float>> vectorSample) {

        this.normalVectors = new ArrayList<>();
        this.midPoints = new ArrayList<>();

        for (int i = 0; i < vectorSample.size(); i += 2 * nbBitsPerTable) {

            List<List<Float>> vA = vectorSample.subList(i, i + nbBitsPerTable);
            List<List<Float>> vB = vectorSample.subList(i + nbBitsPerTable, i + 2 * nbBitsPerTable);

            // Compute the midpoint for each pair of vectors.
            // List<List<Float>> mp = List<>

            System.out.println(vA.toString());
            System.out.println(vB.toString());
            System.out.println(">>>>>>>>>>>>>>>>");
        }

//        System.out.println(nbTables);
//        System.out.println(nbBitsPerTable);
//        System.out.println(nbDimensions);
//        System.out.println(vectorSample);

    }

}
