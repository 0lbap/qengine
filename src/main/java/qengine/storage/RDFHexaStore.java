package qengine.storage;

import fr.boreal.model.logicalElements.api.*;
import org.apache.commons.lang3.NotImplementedException;
import qengine.model.RDFAtom;
import qengine.model.StarQuery;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Implémentation d'un HexaStore pour stocker des RDFAtom.
 * Cette classe utilise six index pour optimiser les recherches.
 * Les index sont basés sur les combinaisons (Sujet, Prédicat, Objet), (Sujet, Objet, Prédicat),
 * (Prédicat, Sujet, Objet), (Prédicat, Objet, Sujet), (Objet, Sujet, Prédicat) et (Objet, Prédicat, Sujet).
 */
public class RDFHexaStore implements RDFStorage {

    private HashMap<Integer, Term> dict = new HashMap<>();

    private List<List<Integer>> atomIndexesSPO = new ArrayList<>();

    private int dictIndex = 1;

    @Override
    public boolean add(RDFAtom atom) {
        var terms = List.of(atom.getTripleSubject(), atom.getTriplePredicate(), atom.getTripleObject());
        List<Integer> indexes = new ArrayList<>(List.of(0, 0, 0));
        for (int i = 0; i < 3; i++) {
            if (dict.containsValue(terms.get(i))) {
                int finalI = i;
                var index = dict.entrySet().stream()
                        .filter(entry -> entry.getValue().equals(terms.get(finalI)))
                        .findFirst()
                        .get()
                        .getKey();
                indexes.set(i, index);
            } else {
                dict.put(dictIndex, terms.get(i));
                indexes.set(i, dictIndex);
                dictIndex++;
            }
        }
        atomIndexesSPO.add(indexes);
        return true;
    }

    @Override
    public long size() {
        return atomIndexesSPO.size();
    }

    @Override
    public Iterator<Substitution> match(RDFAtom atom) {
        throw new NotImplementedException();
    }

    @Override
    public Iterator<Substitution> match(StarQuery q) {
        throw new NotImplementedException();
    }

    @Override
    public List<Atom> getAtoms() {
        return atomIndexesSPO.stream().map(atomIndex -> new RDFAtom(dict.get(atomIndex.get(0)), dict.get(atomIndex.get(1)), dict.get(atomIndex.get(2)))).collect(Collectors.toList());
    }

    @Override
    public String toString() {
        return "HexaStore\n" + "|_ Dictionary: " + dict.toString() + "\n|_ Atom indexes: " + atomIndexesSPO.toString();
    }

}
