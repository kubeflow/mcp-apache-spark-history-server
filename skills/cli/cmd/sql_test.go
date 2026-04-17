package cmd

import "testing"

func TestStripInitialPlans(t *testing.T) {
	tests := []struct {
		name string
		plan string
		want string
	}{
		{
			name: "simple",
			plan: `AdaptiveSparkPlan isFinalPlan=true
+- == Final Plan ==
   ResultQueryStage (13)
   +- * HashAggregate (12)
      +- AQEShuffleRead (11)
+- == Initial Plan ==
   HashAggregate (21)
   +- Exchange (20)
      +- HashAggregate (19)`,
			want: `AdaptiveSparkPlan isFinalPlan=true
+- == Final Plan ==
   ResultQueryStage (13)
   +- * HashAggregate (12)
      +- AQEShuffleRead (11)`,
		},
		{
			name: "nested subquery",
			plan: `AdaptiveSparkPlan isFinalPlan=true
+- == Final Plan ==
   ResultQueryStage (13)
   +- Subquery
      +- AdaptiveSparkPlan isFinalPlan=true
         +- == Final Plan ==
            SomeNode (1)
         +- == Initial Plan ==
            OtherNode (2)
+- == Initial Plan ==
   HashAggregate (21)
   +- Exchange (20)`,
			want: `AdaptiveSparkPlan isFinalPlan=true
+- == Final Plan ==
   ResultQueryStage (13)
   +- Subquery
      +- AdaptiveSparkPlan isFinalPlan=true
         +- == Final Plan ==
            SomeNode (1)`,
		},
		{
			name: "no initial plan",
			plan: `AdaptiveSparkPlan isFinalPlan=false
+- SomeNode (1)
   +- OtherNode (2)`,
			want: `AdaptiveSparkPlan isFinalPlan=false
+- SomeNode (1)
   +- OtherNode (2)`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := stripInitialPlans(tt.plan); got != tt.want {
				t.Errorf("got:\n%s\n\nwant:\n%s", got, tt.want)
			}
		})
	}
}
