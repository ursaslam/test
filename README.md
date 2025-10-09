✅ Step 1: TransformResult<T>

Keeps track of transformation outcome and any errors captured.

import java.util.ArrayList;
import java.util.List;

public class TransformResult<T> {
    private T transformedObject;
    private final List<String> errors = new ArrayList<>();

    public void addError(String field, Exception e) {
        errors.add("Field '" + field + "' failed: " + e.getMessage());
    }

    public void setTransformedObject(T obj) {
        this.transformedObject = obj;
    }

    public T getTransformedObject() {
        return transformedObject;
    }

    public List<String> getErrors() {
        return errors;
    }

    public boolean hasErrors() {
        return !errors.isEmpty();
    }
}

✅ Step 2: BaseTransformer<T>

Defines a safe entry point (safeTransform) that all subclasses inherit.

public abstract class BaseTransformer<T> {

    public TransformResult<T> safeTransform(T input) {
        TransformResult<T> result = new TransformResult<>();

        try {
            transform(input, result); // delegate actual logic
            result.setTransformedObject(input);
        } catch (Exception e) {
            // Catch any unhandled error from child transformer
            result.addError("global", e);
        }

        return result;
    }

    // Each subclass implements this with field-level transformations
    protected abstract void transform(T input, TransformResult<T> result);
}

✅ Step 3: SafeExecutor

Used inside transformers to safely run risky field operations.

public class SafeExecutor {
    public static <T> void safeRun(String fieldName, Runnable action, TransformResult<T> result) {
        try {
            action.run();
        } catch (NullPointerException e) {
            result.addError(fieldName, e);
        } catch (Exception e) {
            result.addError(fieldName, e);
        }
    }
}

✅ Step 4: Example Data Models
public class Customer {
    private String name;
    private Address address;
    private Integer age;

    // Getters and Setters
    public String getName() { return name; }
    public void setName(String name) { this.name = name; }

    public Address getAddress() { return address; }
    public void setAddress(Address address) { this.address = address; }

    public Integer getAge() { return age; }
    public void setAge(Integer age) { this.age = age; }

    @Override
    public String toString() {
        return "Customer{name='" + name + "', age=" + age + ", address=" + address + '}';
    }
}

public class Address {
    private String city;
    private String zip;

    public String getCity() { return city; }
    public void setCity(String city) { this.city = city; }

    public String getZip() { return zip; }
    public void setZip(String zip) { this.zip = zip; }

    @Override
    public String toString() {
        return "Address{city='" + city + "', zip='" + zip + "'}";
    }
}

✅ Step 5: Child Transformers
🔹 AddressTransformer
public class AddressTransformer extends BaseTransformer<Address> {
    @Override
    protected void transform(Address address, TransformResult<Address> result) {
        SafeExecutor.safeRun("city", () -> {
            address.setCity(address.getCity().trim().toUpperCase());
        }, result);

        SafeExecutor.safeRun("zip", () -> {
            address.setZip(address.getZip().replaceAll("-", ""));
        }, result);
    }
}

🔹 CustomerTransformer
public class CustomerTransformer extends BaseTransformer<Customer> {
    private final AddressTransformer addressTransformer = new AddressTransformer();

    @Override
    protected void transform(Customer customer, TransformResult<Customer> result) {
        SafeExecutor.safeRun("name", () -> {
            customer.setName(customer.getName().toUpperCase());
        }, result);

        SafeExecutor.safeRun("age", () -> {
            customer.setAge(customer.getAge() + 1);
        }, result);

        // Nested transformer call
        SafeExecutor.safeRun("address", () -> {
            TransformResult<Address> addrResult = addressTransformer.safeTransform(customer.getAddress());
            result.getErrors().addAll(addrResult.getErrors());
        }, result);
    }
}

✅ Step 6: Orchestrator / Main Flow
import java.util.*;

public class TransformerApp {
    public static void main(String[] args) {
        List<Customer> customers = Arrays.asList(
                makeCustomer("Alice", "New York", "100-01", 30),
                makeCustomer(null, "Boston", "200-02", 25),
                makeCustomer("Charlie", null, "300-03", null)
        );

        CustomerTransformer transformer = new CustomerTransformer();
        List<TransformResult<Customer>> results = new ArrayList<>();

        for (Customer c : customers) {
            TransformResult<Customer> res = transformer.safeTransform(c);
            results.add(res);
        }

        // Print results
        results.forEach(r -> {
            System.out.println("Transformed: " + r.getTransformedObject());
            if (r.hasErrors()) {
                System.out.println("  Errors: " + r.getErrors());
            }
        });
    }

    private static Customer makeCustomer(String name, String city, String zip, Integer age) {
        Customer c = new Customer();
        Address a = new Address();
        a.setCity(city);
        a.setZip(zip);
        c.setAddress(a);
        c.setName(name);
        c.setAge(age);
        return c;
    }
}

✅ Example Output
Transformed: Customer{name='ALICE', age=31, address=Address{city='NEW YORK', zip='10001'}}
Transformed: Customer{name='null', age=26, address=Address{city='BOSTON', zip='20002'}}
  Errors: [Field 'name' failed: Cannot invoke "String.toUpperCase()" because "name" is null]
Transformed: Customer{name='CHARLIE', age=null, address=Address{city='null', zip='30003'}}
  Errors: [Field 'age' failed: Cannot invoke "Integer.intValue()" because "age" is null
