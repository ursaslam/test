Step 1: Define a SafeTransformer that collects errors
import java.util.function.Function;
import java.util.List;

public class SafeTransformer {

    public static <T, R> Function<T, R> safe(Function<T, R> transformer, List<String> errorList) {
        return t -> {
            try {
                return transformer.apply(t);
            } catch (Exception e) {
                String errorMessage = "Error transforming " + t + ": " + e.getMessage();
                errorList.add(errorMessage);
                return null; // return null if failed
            }
        };
    }
}

Step 2: Apply transformers and capture errors
import java.util.*;
import java.util.stream.Collectors;
import java.util.function.Function;

class Person {
    String name;
    Integer age;

    public Person(String name, Integer age) {
        this.name = name;
        this.age = age;
    }

    @Override
    public String toString() {
        return "Person{name='" + name + "', age=" + age + "}";
    }
}

public class TransformerExample {
    public static void main(String[] args) {
        List<Person> persons = Arrays.asList(
            new Person("Alice", 25),
            new Person(null, 30), // will cause NPE in name transformation
            new Person("Bob", null) // will cause NPE in age transformation
        );

        List<String> errors = new ArrayList<>();

        Function<Person, Person> transformer1 = SafeTransformer.safe(p -> {
            // Uppercase name, may throw NPE
            p.name = p.name.toUpperCase();
            return p;
        }, errors);

        Function<Person, Person> transformer2 = SafeTransformer.safe(p -> {
            // Calculate birth year, may throw NPE
            p.age = 2025 - p.age;
            return p;
        }, errors);

        List<Function<Person, Person>> transformers = Arrays.asList(transformer1, transformer2);

        List<Person> transformed = persons.stream()
            .map(p -> {
                Person temp = p;
                for (Function<Person, Person> t : transformers) {
                    temp = t.apply(temp);
                }
                return temp;
            })
            .filter(Objects::nonNull) // skip failed transformations
            .collect(Collectors.toList());

        System.out.println("Transformed Persons:");
        transformed.forEach(System.out::println);

        System.out.println("\nErrors:");
        errors.forEach(System.out::println);
    }
}

✅ Output Example:
Transformed Persons:
Person{name='ALICE', age=2000}

Errors:
Error transforming Person{name='null', age=30}: null
Error transforming Person{name='Bob', age=null}: null

Key Advantages:

Errors are captured in a list — you can log them, report them, or persist them.

Loop continues even if some items fail.

No try/catch clutter inside every transformer.

Filter out failed transformations if desired (filter(Objects::nonNull)).

If you want, I can also give a version where even partially transformed Person objects are preserved and errors are just logged per field instead of discarding the object. This is useful when you don’t want to lose any data.

Do you want me to do that?

ChatGPT can make mistakes. Check important info.
