package concurrent_qengine.storage;

import fr.boreal.model.logicalElements.api.*;
import fr.boreal.model.logicalElements.impl.SubstitutionImpl;
import concurrent_qengine.model.RDFAtom;
import concurrent_qengine.model.StarQuery;

import java.util.*;

/**
 * Implémentation d'un HexaStore pour stocker des RDFAtom.
 * Cette classe utilise six index pour optimiser les recherches.
 * Les index sont basés sur les combinaisons (Sujet, Prédicat, Objet), (Sujet, Objet, Prédicat),
 * (Prédicat, Sujet, Objet), (Prédicat, Objet, Sujet), (Objet, Sujet, Prédicat) et (Objet, Prédicat, Sujet).
 */
public class RDFHexaStore implements RDFStorage {

    HashMap <Integer, Term> dic_int_term = new HashMap<Integer, Term>();
    HashMap <Term, Integer> dic_term_int = new HashMap<Term, Integer>();

    public final Map<Integer, Map<Integer, Set<Integer>>> spo = new HashMap<>();
    public final Map<Integer, Map<Integer, Set<Integer>>> sop = new HashMap<>();
    public final Map<Integer, Map<Integer, Set<Integer>>> pso = new HashMap<>();
    public final Map<Integer, Map<Integer, Set<Integer>>> pos = new HashMap<>();
    public final Map<Integer, Map<Integer, Set<Integer>>> osp = new HashMap<>();
    public final Map<Integer, Map<Integer, Set<Integer>>> ops = new HashMap<>();



    @Override
    public boolean add(RDFAtom atom) {

        boolean isAdded = false;
        int subjectID = -1;
        int predicateID = -1;
        int objectID = -1;

        Term subject = atom.getTripleSubject();
        subjectID = dic_term_int.getOrDefault(subject, -1);
        if (subjectID == -1) {
            subjectID = dic_int_term.size() + 1;
            dic_int_term.put(subjectID, subject);
            dic_term_int.put(subject, subjectID);
            isAdded = true;
        }

        Term predicate = atom.getTriplePredicate();
        predicateID = dic_term_int.getOrDefault(predicate, -1);
        if (predicateID == -1) {
            predicateID = dic_int_term.size() + 1;
            dic_int_term.put(predicateID, predicate);
            dic_term_int.put(predicate, predicateID);
            isAdded = true;
        }

        Term object = atom.getTripleObject();
        objectID = dic_term_int.getOrDefault(object, -1);
        if (objectID == -1) {
            objectID = dic_int_term.size() + 1;
            dic_int_term.put(objectID, object);
            dic_term_int.put(object, objectID);
            isAdded = true;
        }


        Map<Integer, Set<Integer>> spoMap = spo.computeIfAbsent(subjectID, k -> new HashMap<>());
        Set<Integer> spoSet = spoMap.computeIfAbsent(predicateID, k -> new HashSet<>());
        spoSet.add(objectID);

        Map<Integer, Set<Integer>> sopMap = sop.computeIfAbsent(subjectID, k -> new HashMap<>());
        Set<Integer> sopSet = sopMap.computeIfAbsent(objectID, k -> new HashSet<>());
        sopSet.add(predicateID);

        Map<Integer, Set<Integer>> psoMap = pso.computeIfAbsent(predicateID, k -> new HashMap<>());
        Set<Integer> psoSet = psoMap.computeIfAbsent(subjectID, k -> new HashSet<>());
        psoSet.add(objectID);

        Map<Integer, Set<Integer>> posMap = pos.computeIfAbsent(predicateID, k -> new HashMap<>());
        Set<Integer> posSet = posMap.computeIfAbsent(objectID, k -> new HashSet<>());
        posSet.add(subjectID);

        Map<Integer, Set<Integer>> ospMap = osp.computeIfAbsent(objectID, k -> new HashMap<>());
        Set<Integer> ospSet = ospMap.computeIfAbsent(subjectID, k -> new HashSet<>());
        ospSet.add(predicateID);

        Map<Integer, Set<Integer>> opsMap = ops.computeIfAbsent(objectID, k -> new HashMap<>());
        Set<Integer> opsSet = opsMap.computeIfAbsent(predicateID, k -> new HashSet<>());
        opsSet.add(subjectID);



        return isAdded;
    }

    @Override
    public long size() {
        return spo.size();
    }

    @Override
    public Iterator<Substitution> match(RDFAtom atom) {
        Set<Substitution> results = new HashSet<>();

        Term subject = atom.getTripleSubject();
        Term predicate = atom.getTriplePredicate();
        Term object = atom.getTripleObject();

        boolean subjectIsVariable = subject instanceof Variable;
        boolean predicateIsVariable = predicate instanceof Variable;
        boolean objectIsVariable = object instanceof Variable;

        int subjectId = subjectIsVariable ? -1 : dic_term_int.getOrDefault(subject, -1);
        int predicateId = predicateIsVariable ? -1 : dic_term_int.getOrDefault(predicate, -1);
        int objectId = objectIsVariable ? -1 : dic_term_int.getOrDefault(object, -1);


        // Cas 0 : Pas de variables, sujet, prédicat, et objet fixés
        if (!subjectIsVariable && !predicateIsVariable && !objectIsVariable) {
            if (spo.containsKey(subjectId)) {
                Map<Integer, Set<Integer>> predicateMap = spo.get(subjectId);
                if (predicateMap.containsKey(predicateId)) {
                    Set<Integer> objects = predicateMap.get(predicateId);
                    if (objects.contains(objectId)) {
                        Substitution substitution = new SubstitutionImpl();
                        results.add(substitution);
                    }
                }
            }
            else {
                return Collections.emptyIterator();
            }
        }




        // Cas 1 : Variable dans le sujet (S), prédicat et objet fixés
        if (subjectIsVariable && !predicateIsVariable && !objectIsVariable) {
            if (pos.containsKey(predicateId) && pos.get(predicateId).containsKey(objectId)) {
                for (int matchedSubjectId : pos.get(predicateId).get(objectId)) {
                    Term matchedSubject = dic_int_term.get(matchedSubjectId);
                    Substitution substitution = new SubstitutionImpl();
                    substitution.add((Variable) subject, matchedSubject);
                    results.add(substitution);
                }
            }
            else {
                return Collections.emptyIterator();
            }
        }


        // Cas 2 : Variable dans le prédicat (P), sujet et objet fixés
        else if (!subjectIsVariable && predicateIsVariable && !objectIsVariable) {
            if (osp.containsKey(objectId) && osp.get(objectId).containsKey(subjectId)) {
                for (int matchedPredicateId : osp.get(objectId).get(subjectId)) {
                    Term matchedPredicate = dic_int_term.get(matchedPredicateId);

                    Substitution substitution = new SubstitutionImpl();
                    substitution.add((Variable) predicate, matchedPredicate);
                    results.add(substitution);
                }
            }
            else {
                return Collections.emptyIterator();
            }
        }

        // Cas 3 : Variable dans l'objet (O), sujet et prédicat fixés
        else if (!subjectIsVariable && !predicateIsVariable && objectIsVariable) {
            if (pso.containsKey(predicateId) && pso.get(predicateId).containsKey(subjectId)) {
                for (int matchedObjectId : pso.get(predicateId).get(subjectId)) {
                    Term matchedObject = dic_int_term.get(matchedObjectId);

                    Substitution substitution = new SubstitutionImpl();
                    substitution.add((Variable) object, matchedObject);
                    results.add(substitution);
                }
            }
            else {
                return Collections.emptyIterator();
            }
        }

        // Cas 4 : Variables dans le prédicat (P) et l'objet (O), sujet fixé
        else if (!subjectIsVariable && predicateIsVariable && objectIsVariable) {
            if (spo.containsKey(subjectId)) {
                for (Map.Entry<Integer, Set<Integer>> predicateEntry : spo.get(subjectId).entrySet()) {
                    int matchedPredicateId = predicateEntry.getKey();
                    Term matchedPredicate = dic_int_term.get(matchedPredicateId);

                    for (int matchedObjectId : predicateEntry.getValue()) {
                        Term matchedObject = dic_int_term.get(matchedObjectId);

                        Substitution substitution = new SubstitutionImpl();
                        substitution.add((Variable) object, matchedObject);
                        substitution.add((Variable) predicate, matchedPredicate);
                        results.add(substitution);
                    }
                }
            }
            else {
                return Collections.emptyIterator();
            }
        }



        // Cas 4 : Variables dans l'objet (O) et le sujet (S), prédicat fixé
        else if (subjectIsVariable && !predicateIsVariable && objectIsVariable) {
            if (pos.containsKey(predicateId)) {
                for (Map.Entry<Integer, Set<Integer>> entry : pos.get(predicateId).entrySet()) {
                    int matchedObjectId = entry.getKey();
                    Term matchedObject = dic_int_term.get(matchedObjectId);

                    for (int matchedSubjectId : entry.getValue()) {
                        Term matchedSubject = dic_int_term.get(matchedSubjectId);

                        Substitution substitution = new SubstitutionImpl();
                        substitution.add((Variable) subject, matchedSubject);
                        substitution.add((Variable) object, matchedObject);
                        results.add(substitution);
                    }
                }
            }
            else {
                return Collections.emptyIterator();
            }
        }

        // Cas 5 : Variables dans le prédicat (P) et le sujet (S), objet fixé
        else if (subjectIsVariable && predicateIsVariable && !objectIsVariable) {
            if (ops.containsKey(objectId)) {
                for (Map.Entry<Integer, Set<Integer>> entry : ops.get(objectId).entrySet()) {
                    int matchedPredicateId = entry.getKey();
                    Term matchedPredicate = dic_int_term.get(matchedPredicateId);

                    for (int matchedSubjectId : entry.getValue()) {
                        Term matchedSubject = dic_int_term.get(matchedSubjectId);

                        Substitution substitution = new SubstitutionImpl();
                        substitution.add((Variable) subject, matchedSubject);
                        substitution.add((Variable) predicate, matchedPredicate);
                        results.add(substitution);
                    }
                }
            }
            else {
                return Collections.emptyIterator();
            }
        }

        // Cas 6 : Variables dans le prédicat (P), le sujet (S), et l'objet (O)
        else if (subjectIsVariable && predicateIsVariable && objectIsVariable) {
            for (Map.Entry<Integer, Map<Integer, Set<Integer>>> subjectEntry : spo.entrySet()) {
                int matchedSubjectId = subjectEntry.getKey();
                Term matchedSubject = dic_int_term.get(matchedSubjectId);

                for (Map.Entry<Integer, Set<Integer>> predicateEntry : subjectEntry.getValue().entrySet()) {
                    int matchedPredicateId = predicateEntry.getKey();
                    Term matchedPredicate = dic_int_term.get(matchedPredicateId);

                    for (int matchedObjectId : predicateEntry.getValue()) {
                        Term matchedObject = dic_int_term.get(matchedObjectId);

                        Substitution substitution = new SubstitutionImpl();
                        substitution.add((Variable) subject, matchedSubject);
                        substitution.add((Variable) predicate, matchedPredicate);
                        substitution.add((Variable) object, matchedObject);
                        results.add(substitution);
                    }
                }
            }
        }

        return results.iterator();
    }



    @Override
    public Iterator<Substitution> match(StarQuery q) {
        ArrayList<Substitution> results = new ArrayList<>();
        ArrayList<Substitution> answersShortestQuery = new ArrayList<>();
        int nbAnswers = 0;
        Iterator<Substitution> tempSubstitutions;
        RDFAtom shortestAtom = null;

        List<RDFAtom> atoms = q.getRdfAtoms();
        // 1ère boucle pour récupérer la liste avec le moins de résultats
        for (RDFAtom atom : atoms) {

            tempSubstitutions =  match(atom);
            ArrayList<Substitution> answers = new ArrayList<>();
            tempSubstitutions.forEachRemaining(answers::add);


            // 1ère itération
            if(answersShortestQuery.isEmpty()) {
                answersShortestQuery = answers;
                nbAnswers = answers.size();
                shortestAtom = atom;
            } else {
                // Si liste avec le moins de résultats -> met dans la variable substitutions

                if(answers.size() < nbAnswers)  {
                    nbAnswers = answers.size();
                    answersShortestQuery = answers;
                    shortestAtom = atom;
                }
            }
        }

        // 2ème boucle pour comparer tout ça
        for (Substitution sub : answersShortestQuery) {
            boolean estBon = true;
            for (RDFAtom atom : atoms) {
                if(!atom.equals(shortestAtom)) {
                    Term aTesterTerm = sub.toMap().get(q.getCentralVariable());
                    //Literal<String> aTester = (Literal<String>) aTesterTerm;
                    RDFAtom newAtom = new RDFAtom(aTesterTerm, atom.getTriplePredicate(), atom.getTripleObject());
                    Iterator<Substitution> reponse = match(newAtom);
                    if(!reponse.hasNext()) {
                        estBon = false;
                        break;
                    }
                }
            }
            if(estBon) {
                results.add(sub);
            }
        }
        /* Pour tests bloquants de correction / complétude -> on renvoie tjr une liste  vide
        ArrayList<Substitution> results2 = new ArrayList<>();
        return results2.iterator();
        */
        return results.iterator();

    }

    @Override
    public Collection<Atom> getAtoms() {
        List<Atom> atoms = new ArrayList<>();

        spo.forEach((subjectId, predicateMap) -> {
            Term subject = dic_int_term.get(subjectId);

            predicateMap.forEach((predicateId, objectSet) -> {
                Term predicate = dic_int_term.get(predicateId);

                objectSet.forEach(objectId -> {
                    Term object = dic_int_term.get(objectId);
                    atoms.add(new RDFAtom(subject, predicate, object));
                });
            });
        });

        return atoms;
    }

}
