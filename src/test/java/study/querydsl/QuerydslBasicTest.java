package study.querydsl;

import com.fasterxml.jackson.databind.deser.std.StdKeyDeserializer;
import com.querydsl.core.QueryResults;
import com.querydsl.core.Tuple;
import com.querydsl.core.types.Expression;
import com.querydsl.core.types.Projections;
import com.querydsl.core.types.dsl.CaseBuilder;
import com.querydsl.core.types.dsl.Expressions;
import com.querydsl.core.types.dsl.NumberExpression;
import com.querydsl.jpa.JPAExpressions;
import com.querydsl.jpa.impl.JPAQuery;
import com.querydsl.jpa.impl.JPAQueryFactory;
import jakarta.persistence.EntityManager;
import jakarta.persistence.EntityManagerFactory;
import jakarta.persistence.PersistenceUnit;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.transaction.annotation.Transactional;
import study.querydsl.dto.MemberDto;
import study.querydsl.dto.UserDto;
import study.querydsl.entity.Member;
import study.querydsl.entity.QMember;
import study.querydsl.entity.QTeam;
import study.querydsl.entity.Team;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static study.querydsl.entity.QMember.member;
import static study.querydsl.entity.QTeam.team;

@SpringBootTest
@Transactional
public class QuerydslBasicTest {
    @Autowired
    EntityManager entityManager;
    JPAQueryFactory queryFactory;

    @PersistenceUnit
    EntityManagerFactory emf;

    @BeforeEach
    public void before() {
        queryFactory = new JPAQueryFactory(entityManager);
        Team teamA = new Team("teamA");
        Team teamB = new Team("teamB");
        entityManager.persist(teamA);
        entityManager.persist(teamB);
        Member member1 = new Member("member1", 10, teamA);
        Member member2 = new Member("member2", 20, teamA);
        Member member3 = new Member("member3", 30, teamB);
        Member member4 = new Member("member4", 40, teamB);
        entityManager.persist(member1);
        entityManager.persist(member2);
        entityManager.persist(member3);
        entityManager.persist(member4);
    }

    @Test
    public void startJPQL(){
        String qlString = "select m from Member m where m.username = :username";
        Member findMember = entityManager.createQuery(qlString,Member.class)
                .setParameter("username","member1")
                .getSingleResult();
        assertThat(findMember.getUsername()).isEqualTo("member1");
    }

    @Test
    public void startQuerydsl() {
//        JPAQueryFactory queryFactory = new JPAQueryFactory(entityManager); 클래스의 멤버변수로 정의하여, 메서드 안에서는 주석처리함.
//        QMember member = QMember.member; QMember를 static import 처리하여, 주석처리가 가능해졌음. import static study.querydsl.entity.QMember.member;
        Member findMember = queryFactory
                .select(member)
                .from(member)
                .where(member.username.eq("member1"))
                .fetchOne();
        assertThat(findMember.getUsername()).isEqualTo("member1");
    }


    @Test
    public void search() {
        Member findMember = queryFactory
                .selectFrom(member)
                .where(member.username.eq("member1")
                        .and(member.age.eq(10)))
                .fetchOne();
        assertThat(findMember.getUsername()).isEqualTo("member1");
    }

    @Test
    public void searchAndParam() {
        Member findMember = queryFactory.selectFrom(member)
                .where(
                        member.username.eq("member1"),
                        member.age.eq(10)
                )
                .fetchOne();
        assertThat(findMember.getUsername()).isEqualTo("member1");
    }

    @Test
    public void resultFetch() {
//        QueryResults<Member> results =
//                queryFactory.selectFrom(member)
//                .fetchResults();
//        results.getTotal();
//        List<Member> contents = results.getResults();

        long count = queryFactory.selectFrom(member).fetchCount();

    }

    /** 정렬 */
    @Test
    public void sort() {
        entityManager.persist(new Member(null,100));
        entityManager.persist(new Member("member5",100));
        entityManager.persist(new Member("member6",100));

        List<Member> result = queryFactory.selectFrom(member)
                .where(member.age.eq(100))
                .orderBy(member.age.desc(), member.username.asc().nullsLast())
                .fetch();

        Member member5 = result.get(0);
        Member member6 = result.get(1);
        Member memberNull = result.get(2);

        assertThat(member5.getUsername()).isEqualTo("member5");
        assertThat(member6.getUsername()).isEqualTo("member6");
        assertThat(memberNull.getUsername()).isNull();
    }

    @Test
    public void pagingLimitCount() {
        List<Member> fetch = queryFactory
                .selectFrom(member)
                .orderBy(member.username.desc())
                .offset(1)
                .limit(2)
                .fetch();
        assertThat(fetch.size()).isEqualTo(2);
    }

    @Test
    public void pagingTotalCount() {
        QueryResults<Member> memberQueryResults = queryFactory
                .selectFrom(member)
                .orderBy(member.username.desc())
                .offset(1)
                .limit(2)
                .fetchResults();
        assertThat(memberQueryResults.getTotal()).isEqualTo(4);
        assertThat(memberQueryResults.getLimit()).isEqualTo(2);
        assertThat(memberQueryResults.getOffset()).isEqualTo(1);
        assertThat(memberQueryResults.getResults().size()).isEqualTo(2);
    }


    @Test
    public void aggregation() {
        List<Tuple> fetchResult = queryFactory
                .select(
                        member.count(),
                        member.age.sum(),
                        member.age.avg(),
                        member.age.max(),
                        member.age.min()
                )
                .from(member)
                .fetch();

        Tuple tuple = fetchResult.get(0);
        assertThat(tuple.get(member.count())).isEqualTo(4);
        assertThat(tuple.get(member.age.sum())).isEqualTo(100);
        assertThat(tuple.get(member.age.avg())).isEqualTo(25);
        assertThat(tuple.get(member.age.max())).isEqualTo(40);
        assertThat(tuple.get(member.age.min())).isEqualTo(10);

    }

    @Test
    public void group() throws Exception {
        List<Tuple> result = queryFactory
                .select(team.name, member.age.avg())
                .from(member)
                .join(member.team, team) // target: member.team, aliasing으로 사용 할 Q타입: team(QTeam)
                .groupBy(team.name)
                .fetch();

        Tuple teamA = result.get(0);
        Tuple teamB = result.get(1);

        assertThat(teamA.get(team.name)).isEqualTo("teamA");
        assertThat(teamA.get(member.age.avg())).isEqualTo(15);
        assertThat(teamB.get(team.name)).isEqualTo("teamB");
        assertThat(teamB.get(member.age.avg())).isEqualTo(35);

    }

    @Test
    public void join() throws Exception {
        QMember member = QMember.member;
        QTeam team = QTeam.team;

        List<Member> result = queryFactory.selectFrom(member)
                .join(member.team, team) // member.team 테이블을 member 테이블과 조인, team은 member.team의 aliasing
                .where(team.name.eq("teamA"))
                .fetch();
        assertThat(result)
                .extracting("username")
                .containsExactly("member1","member2");


    }

    /**
     * 세타 조인(연관관계가 없는 필드로 조인)
     * 회원의 이름이 팀 이름과 같은 회원 조회
     */
    @Test
    public void thetaJoin() throws Exception {
        entityManager.persist(new Member("teamA"));
        entityManager.persist(new Member("teamB"));

        List<Member> result = queryFactory
                .select(member)
                .from(member, team)
                .where(member.username.eq(team.name))
                .fetch();
        assertThat(result)
                .extracting("username")
                .containsExactly("teamA", "teamB");
    }


    /**
     * 예) 회원과 팀을 조인하면서, 팀 이름이 teamA인 팀만 조인, 회원은 모두 조회
     * JPQL: SELECT m, t FROM Member m LEFT JOIN m.team t on t.name = 'teamA'
     * SQL: SELECT m.*, t.* FROM Member m LEFT JOIN Team t ON m.TEAM_ID=t.id and
     t.name='teamA'

     참고: on 절을 활용해 조인 대상을 필터링 할 때, 외부조인이 아니라 내부조인(inner join)을 사용하면,
     where 절에서 필터링 하는 것과 기능이 동일하다. 따라서 on 절을 활용한 조인 대상 필터링을 사용할 때,
     내부조인 이면 익숙한 where 절로 해결하고, 정말 외부조인이 필요한 경우에만 이 기능을 사용하자.
     */
    @Test
    public void joinOnFiltering() throws Exception {
        List<Tuple> result = queryFactory
                .select(member, team)
                .from(member)
                .leftJoin(member.team, team).on(team.name.eq("teamA"))
                .fetch();

        for (Tuple tuple : result) {
            System.out.println("tuple = " + tuple);
        }

    }

    /**
     *2. 연관관계 없는 엔티티 외부 조인
     *예)회원의 이름과 팀의 이름이 같은 대상 외부 조인
     * JPQL: SELECT m, t FROM Member m LEFT JOIN Team t on m.username = t.name
     * SQL: SELECT m.*, t.* FROM Member m LEFT JOIN Team t ON m.username = t.name */
    @Test
    public void join_on_no_relation() throws Exception {
        entityManager.persist(new Member("teamA"));
        entityManager.persist(new Member("teamB"));
        List<Tuple> result = queryFactory
                .select(member, team)
                .from(member)
                .leftJoin(team).on(member.username.eq(team.name))
                .fetch();
        for (Tuple tuple : result) {
            System.out.println("t=" + tuple);
        }
    }

    /**
     * 페치조인 미적용
     * 지연로딩으로 Member, Team SQL 쿼리 각각 실행
     *
     * @throws Exception
     */
    @Test
    public void noneFetchJoin() throws Exception {
        entityManager.flush();
        entityManager.clear();

        Member findMember = queryFactory
                .selectFrom(member)
                .where(member.username.eq("member1"))
                .fetchOne();
        boolean loaded =
                emf.getPersistenceUnitUtil().isLoaded(findMember.getTeam());
        assertThat(loaded).as("페치 조인 미적용").isFalse();
    }

    /**
     * 페치 조인 적용
     * 즉시로딩으로 Member, Team SQL 쿼리 조인으로 한번에 조회
     * join(), leftJoin() 등 조인 기능 뒤에 fetchJoin() 이라고 추가하면 된다.
     * @throws Exception
     */
    @Test
    public void useFetchJoin() throws Exception {
        entityManager.flush();
        entityManager.clear();

        Member findMember = queryFactory
                .selectFrom(member)
                .join(member.team, team).fetchJoin()
                .where(member.username.eq("member1"))
                .fetchOne();

        boolean loaded =
                emf.getPersistenceUnitUtil().isLoaded(findMember.getTeam());
        assertThat(loaded).as("페치 조인 적용").isTrue();

    }

    /*****************************************************************/
    /** sub query는 select절과 where절에서만 사용 가능, from절에서는 사용 불가. */
    /*****************************************************************/
    /**
     * where절의 Sub query
     * 나이가 가장 많은 회원 조회
     *
     * @throws Exception
     */
    @Test
    public void subQuery() throws Exception{
        /* sub query에 사용한 QMember에서 사용하기 위해 인스턴스를 생성한다. */
        QMember memberSub = new QMember("memberSub");

        List<Member> result = queryFactory.selectFrom(member)
                .where(member.age.eq(
                        JPAExpressions
                                .select(memberSub.age.max())
                                .from(memberSub)
                ))
                .fetch();
        assertThat(result).extracting("age")
                .containsExactly(40);
    }

    /***
     * where절의 Sub query
     * 나이가 평균 나이 이상인 회원
     *
     * @throws Exception
     */
    @Test
    public void subQueryGoe() throws Exception {
        /* sub query에 사용한 QMember에서 사용하기 위해 인스턴스를 생성한다. */
        QMember memberSub = new QMember("memberSub");

        List<Member> result = queryFactory
                .selectFrom(member)
                .where(member.age.goe(
                        JPAExpressions
                                .select(memberSub.age.avg())
                                .from(memberSub)
                ))
                .fetch();
        assertThat(result).extracting("age")
                .containsExactly(30,40);
    }

    /**
     * 서브쿼리 여러 건 처리, in 사용
     * @throws Exception
     */
    public void subQueryIn() throws Exception {
        QMember memberSub = new QMember("memberSub");

        List<Member> result = queryFactory
                .selectFrom(member)
                .where(member.age.in(
                        JPAExpressions
                                .select(memberSub.age)
                                .from(memberSub)
                                .where(memberSub.age.gt(10))
                ))
                .fetch();
        assertThat(result).extracting("age")
                .containsExactly(20,30,40);
    }

    /**
     * select 절에 subquery
     * @throws Exception
     */
    @Test
    public void subQuerySelect() throws Exception {
        QMember memberSub = new QMember("memberSub");
        List<Tuple> fetch = queryFactory
                .select(member.username,
                JPAExpressions
                        .select(member.age.avg())
                        .from(memberSub)
        ).from(member)
        .fetch();

        for (Tuple tuple : fetch) {
            System.out.println("username = " + tuple.get(member.username));
            System.out.println("age = " +
                    tuple.get(JPAExpressions.select(memberSub.age.avg())
                            .from(memberSub)));
        }

    }

    /** Case구문 단순조건 */
    @Test
    public void singleCaseQuery() {
        List<String> result = queryFactory
                .select(
                        member.age.when(10).then("열살")
                                .when(20).then("스무살")
                                .otherwise("기타"))
                .from(member)
                .fetch();
    }

    /** Case구문 복합조건 */
    @Test
    public void multiCaseQuery() {
        List<String> result = queryFactory
                .select(new CaseBuilder()
                        .when(member.age.between(0,20)).then("0~20살")
                        .when(member.age.between(21,30)).then("21~30살")
                        .otherwise("기타"))
                .from(member).fetch();
    }

    /** orderBy에서 Case 문 함께 사용하기 */
    @Test
    public void orderByCaseQuery() throws Exception{
        // - order by에서 사용할 case문 작성
        NumberExpression<Integer> rankPath = new CaseBuilder()
                .when(member.age.between(0,20)).then(2)
                .when(member.age.between(21,30)).then(1)
                .otherwise(3);

        List<Tuple> result = queryFactory
                .select(member.username, member.age, rankPath)
                .from(member)
                .orderBy(rankPath.desc())
                .fetch();

        for (Tuple tuple : result) {
            String username = tuple.get(member.username);
            Integer age = tuple.get(member.age);
            Integer rank = tuple.get(rankPath);
            System.out.println("username = " + username + " age = " + age + " rank = " + rank);
        }
        
    }

    @Test
    public void constantQuery() {
        Tuple result = queryFactory
                .select(member.username, Expressions.constant("A"))
                .from(member)
                .fetchFirst();
        System.out.println("result = " + result);
    }

    /** 문자 더하기 concat */
    @Test
    public void concatQuery() {
        String result = queryFactory
                .select(member.username.concat("_").concat(member.age.stringValue()))
                .from(member)
                .where(member.username.eq("member1"))
                .fetchOne();
        System.out.println("result = " + result);
    }

    /** 단일 프로젝션 조회 */
    @Test
    public void singleProjection() {
        List<String> result = queryFactory
                .select(member.username)
                .from(member)
                .fetch();
        System.out.println("result = " + result);
    }

    @Test
    public void multiProjection() {
        List<Tuple> result = queryFactory
                .select(member.username, member.age)
                .from(member)
                .fetch();

        result.forEach(tuple -> {
            String username = tuple.get(member.username);
            Integer age = tuple.get(member.age);
            System.out.println("username=" + username);
            System.out.println("age=" + age);
        });

//        for (Tuple tuple : result) {
//            String username = tuple.get(member.username);
//            Integer age = tuple.get(member.age);
//            System.out.println("username=" + username);
//            System.out.println("age=" + age);
//
//        }
    }
    /*****************************************************************/
    /** 결과를 DTO 반환할 때 사용 - 3가지 */
    /*****************************************************************/
    /**
     * 1. 프로퍼티 접근: MemberDto의 setter메서드를 통해, 반환 값을 구성
     *    - 해당 DTO(MemberDto)에 기본 생성자 및 setter 메서드가 있어야한다.
     *      . @Data가 있다면, setter메서드 생략가능
     *      . @NoArgsConstructor가 있다면, 기본생성자 생략가능
     * */
    @Test
    public void findByQuerydslSetter() {
        List<MemberDto> result = queryFactory
                .select(Projections.bean(MemberDto.class,
                        member.username,
                        member.age
                ))
                .from(member)
                .fetch();
    }

    /**
     * 2. 필드 직접 접근: MemberDto의 필드(멤버필드)를 통해, 반환 값을 구성
     */
    @Test
    public void findByQuerydslFields() {
        List<MemberDto> result = queryFactory
                .select(Projections.fields(MemberDto.class,
                        member.username,
                        member.age
                ))
                .from(member)
                .fetch();
    }

    /**
     * 3. 생성자 사용: MemberDto의 생성자를 통해, 반환 값을 구성
     */
    @Test
    public void findByQuerydslConstructor() {
        List<MemberDto> result = queryFactory
                .select(Projections.constructor(MemberDto.class,
                        member.username,
                        member.age))
                .from(member)
                .fetch();
    }

    /**
     * 별칭이 다를때 (조회한 필드의 명을 별도로 설정)
     *
     * 반환타입이 MemberDto에서 UserDto로 변경되었을때 별칭을 주어 처리.
     *
     */
    public void findByQuerydslAliasing() {
        /*
         * MemberDto 반환유형을 UserDto 반환유형으로 변경된다면..
         *   . MemberDto.username 속성을 UserDto에 있는 name 으로 aliasing 한다.
         *   . UserDto.age 속성에 Member엔티티의 age속성 Max 값을 할당하고, age라고 aliasing 한다.
         *
         */
        QMember memberSub = new QMember("memberSub");
        queryFactory
                .select(Projections.fields(UserDto.class,
                        member.username.as("name"),
                        Expressions.as(
                                JPAExpressions
                                        .select(memberSub.age.max())
                                        .from(memberSub), "age")
                        )
                )
                .from(member)
                .fetch();
    }


}






























