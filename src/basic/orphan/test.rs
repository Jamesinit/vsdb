use super::*;

#[test]
fn test_compare() {
    assert_eq!(Orphan::new(0), 0);
    assert!(Orphan::new(111) > 0);
    assert!(Orphan::new(111) >= 0);
    assert!(Orphan::new(0) < 111);
    assert!(Orphan::new(0) <= 111);
}

#[test]
fn test_calc() {
    assert_eq!(Orphan::new(111) + 111, 222);
    assert_eq!(Orphan::new(111) - 111, 0);
    assert_eq!(Orphan::new(111) * 111, 111 * 111);
    assert_eq!(Orphan::new(111) / 2, 55);
    assert_eq!(Orphan::new(111) % 2, 1);

    assert_eq!(-Orphan::new(111), -111);
    assert_eq!(!Orphan::new(111), !111);

    assert_eq!(Orphan::new(111) >> 2, 111 >> 2);
    assert_eq!(Orphan::new(111) << 2, 111 << 2);

    assert_eq!(Orphan::new(111) | 2, 111 | 2);
    assert_eq!(Orphan::new(111) & 2, 111 & 2);
    assert_eq!(Orphan::new(111) ^ 2, 111 ^ 2);
}
#[test]
fn test_mut() {
    let mut v = Orphan::new(1);
    v += 1;
    assert_eq!(v, 2);
    v *= 100;
    assert_eq!(v, 200);
    v -= 1;
    assert_eq!(v, 199);
    v /= 10;
    assert_eq!(v, 19);
    v %= 10;
    assert_eq!(v, 9);

    *v.get_mut() = -v.get_value();
    assert_eq!(v, -9);
}

#[test]
fn custom_types() {
    #[derive(
        Default, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Deserialize, Serialize,
    )]
    struct Foo {
        a: i32,
        b: String,
        c: bool,
    }

    assert_eq!(Orphan::new(Foo::default()), Foo::default());
    assert_eq!(Orphan::new(Foo::default()), Orphan::new(Foo::default()));

    assert!(
        Orphan::new(Foo::default())
            < Foo {
                a: 1,
                b: "".to_string(),
                c: true
            }
    );
    assert!(
        Orphan::new(Foo::default())
            <= Foo {
                a: 1,
                b: "".to_string(),
                c: true
            }
    );

    assert!(Orphan::new(Foo::default()) >= Foo::default());
    assert!(Orphan::new(Foo::default()) >= Orphan::new(Foo::default()));
}
