// Copyright 2023 Specter Ops, Inc.
//
// Licensed under the Apache License, Version 2.0
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// SPDX-License-Identifier: Apache-2.0

package fixtures

import (
	"bytes"

	"github.com/specterops/dawgs/cypher/models/cypher/format"

	"github.com/specterops/bloodhound/cmd/api/src/test"
	"github.com/specterops/bloodhound/packages/go/graphschema/ad"
	"github.com/specterops/bloodhound/packages/go/graphschema/common"
	"github.com/specterops/dawgs/cypher/models/cypher"
	"github.com/specterops/dawgs/graph"
	"github.com/specterops/dawgs/query"
	"github.com/stretchr/testify/require"
)

type PropertyAssertion struct {
	ObjectID      string
	Property      string
	ExpectedValue any
}

var (
	ingestRelationshipAssertionCriteria = []graph.Criteria{
		//// DOMAINS
		query.And(
			query.Kind(query.Start(), ad.GPO),
			query.Equals(query.StartProperty(common.ObjectID.String()), "BE91688F-1333-45DF-93E4-4D2E8A36DE2B"),
			query.Kind(query.Relationship(), ad.GPLink),
			query.Kind(query.End(), ad.Domain),
			query.Equals(query.EndProperty(common.ObjectID.String()), "S-1-5-21-3130019616-2776909439-2417379446")),
		query.And(
			query.Kind(query.Start(), ad.Group),
			query.Equals(query.StartProperty(common.ObjectID.String()), "TESTLAB.LOCAL-S-1-5-32-544"),
			query.Kind(query.Relationship(), ad.Owns),
			query.Kind(query.End(), ad.Domain),
			query.Equals(query.EndProperty(common.ObjectID.String()), "S-1-5-21-3130019616-2776909439-2417379446")),
		query.And(
			query.Kind(query.Start(), ad.User),
			query.Equals(query.StartProperty(common.ObjectID.String()), "S-1-5-21-3130019616-2776909439-2417379446-2122"),
			query.Kind(query.Relationship(), ad.DCSync),
			query.Kind(query.End(), ad.Domain),
			query.Equals(query.EndProperty(common.ObjectID.String()), "S-1-5-21-3130019616-2776909439-2417379446")),
		query.And(
			query.Kind(query.Start(), ad.User),
			query.Equals(query.StartProperty(common.ObjectID.String()), "S-1-5-21-3130019616-2776909439-2417379446-2122"),
			query.Kind(query.Relationship(), ad.GetChanges),
			query.Kind(query.End(), ad.Domain),
			query.Equals(query.EndProperty(common.ObjectID.String()), "S-1-5-21-3130019616-2776909439-2417379446")),
		query.And(
			query.Kind(query.Start(), ad.User),
			query.Equals(query.StartProperty(common.ObjectID.String()), "S-1-5-21-3130019616-2776909439-2417379446-2122"),
			query.Kind(query.Relationship(), ad.GetChangesAll),
			query.Kind(query.End(), ad.Domain),
			query.Equals(query.EndProperty(common.ObjectID.String()), "S-1-5-21-3130019616-2776909439-2417379446")),
		//TODO: These new tests are not valid, we need to investigate with Jonas
		// query.And( // ParentChild outbound
		// 	query.Kind(query.Start(), ad.Domain),
		// 	query.Equals(query.StartProperty(common.ObjectID.String()), "S-1-5-21-3130019616-2776909439-2417379446"),
		// 	query.Kind(query.Relationship(), ad.SameForestTrust),
		// 	query.Kind(query.End(), ad.Domain),
		// 	query.Equals(query.EndProperty(common.ObjectID.String()), "S-1-5-21-3084884204-958224920-2707782875")),
		// query.And( // ParentChild inbound
		// 	query.Kind(query.Start(), ad.Domain),
		// 	query.Equals(query.StartProperty(common.ObjectID.String()), "S-1-5-21-3084884204-958224920-2707782875"),
		// 	query.Kind(query.Relationship(), ad.SameForestTrust),
		// 	query.Kind(query.End(), ad.Domain),
		// 	query.Equals(query.EndProperty(common.ObjectID.String()), "S-1-5-21-3130019616-2776909439-2417379446")),
		// query.And( // Treeroot outbound
		// 	query.Kind(query.Start(), ad.Domain),
		// 	query.Equals(query.StartProperty(common.ObjectID.String()), "S-1-5-21-3130019616-2776909439-2417379446"),
		// 	query.Kind(query.Relationship(), ad.SameForestTrust),
		// 	query.Kind(query.End(), ad.Domain),
		// 	query.Equals(query.EndProperty(common.ObjectID.String()), "S-1-5-21-3084884204-958224920-2707782876")),
		// query.And( // Treeroot inbound
		// 	query.Kind(query.Start(), ad.Domain),
		// 	query.Equals(query.StartProperty(common.ObjectID.String()), "S-1-5-21-3084884204-958224920-2707782876"),
		// 	query.Kind(query.Relationship(), ad.SameForestTrust),
		// 	query.Kind(query.End(), ad.Domain),
		// 	query.Equals(query.EndProperty(common.ObjectID.String()), "S-1-5-21-3130019616-2776909439-2417379446")),
		// query.And( // CrossLink outbound
		// 	query.Kind(query.Start(), ad.Domain),
		// 	query.Equals(query.StartProperty(common.ObjectID.String()), "S-1-5-21-3130019616-2776909439-2417379446"),
		// 	query.Kind(query.Relationship(), ad.SameForestTrust),
		// 	query.Kind(query.End(), ad.Domain),
		// 	query.Equals(query.EndProperty(common.ObjectID.String()), "S-1-5-21-3084884204-958224920-2707782877")),
		// query.And( // Forest outbound
		// 	query.Kind(query.Start(), ad.Domain),
		// 	query.Equals(query.StartProperty(common.ObjectID.String()), "S-1-5-21-3130019616-2776909439-2417379446"),
		// 	query.Kind(query.Relationship(), ad.CrossForestTrust),
		// 	query.Kind(query.End(), ad.Domain),
		// 	query.Equals(query.EndProperty(common.ObjectID.String()), "S-1-5-21-3084884204-958224920-2707782878")),
		// query.And( // SpoofSIDHistory inbound (based on forest outbound)
		// 	query.Kind(query.Start(), ad.Domain),
		// 	query.Equals(query.StartProperty(common.ObjectID.String()), "S-1-5-21-3084884204-958224920-2707782878"),
		// 	query.Kind(query.Relationship(), ad.SpoofSIDHistory),
		// 	query.Kind(query.End(), ad.Domain),
		// 	query.Equals(query.EndProperty(common.ObjectID.String()), "S-1-5-21-3130019616-2776909439-2417379446")),
		// query.And( // External inbound
		// 	query.Kind(query.Start(), ad.Domain),
		// 	query.Equals(query.StartProperty(common.ObjectID.String()), "S-1-5-21-3084884204-958224920-2707782879"),
		// 	query.Kind(query.Relationship(), ad.CrossForestTrust),
		// 	query.Kind(query.End(), ad.Domain),
		// 	query.Equals(query.EndProperty(common.ObjectID.String()), "S-1-5-21-3130019616-2776909439-2417379446")),
		// query.And( // Kerberos outbound
		// 	query.Kind(query.Start(), ad.Domain),
		// 	query.Equals(query.StartProperty(common.ObjectID.String()), "S-1-5-21-3130019616-2776909439-2417379446"),
		// 	query.Kind(query.Relationship(), ad.CrossForestTrust),
		// 	query.Kind(query.End(), ad.Domain),
		// 	query.Equals(query.EndProperty(common.ObjectID.String()), "S-1-5-21-3084884204-958224920-2707782880")),
		// query.And( // Kerberos inbound
		// 	query.Kind(query.Start(), ad.Domain),
		// 	query.Equals(query.StartProperty(common.ObjectID.String()), "S-1-5-21-3084884204-958224920-2707782880"),
		// 	query.Kind(query.Relationship(), ad.CrossForestTrust),
		// 	query.Kind(query.End(), ad.Domain),
		// 	query.Equals(query.EndProperty(common.ObjectID.String()), "S-1-5-21-3130019616-2776909439-2417379446")),
		// query.And( // SpoofSIDHistory inbound (based on Kerberos outbound)
		// 	query.Kind(query.Start(), ad.Domain),
		// 	query.Equals(query.StartProperty(common.ObjectID.String()), "S-1-5-21-3084884204-958224920-2707782880"),
		// 	query.Kind(query.Relationship(), ad.SpoofSIDHistory),
		// 	query.Kind(query.End(), ad.Domain),
		// 	query.Equals(query.EndProperty(common.ObjectID.String()), "S-1-5-21-3130019616-2776909439-2417379446")),
		// query.And( // Unknown outbound
		// 	query.Kind(query.Start(), ad.Domain),
		// 	query.Equals(query.StartProperty(common.ObjectID.String()), "S-1-5-21-3130019616-2776909439-2417379446"),
		// 	query.Kind(query.Relationship(), ad.CrossForestTrust),
		// 	query.Kind(query.End(), ad.Domain),
		// 	query.Equals(query.EndProperty(common.ObjectID.String()), "S-1-5-21-3084884204-958224920-2707782881")),
		query.And(
			query.Kind(query.Start(), ad.Domain),
			query.Equals(query.StartProperty(common.ObjectID.String()), "S-1-5-21-3130019616-2776909439-2417379446"),
			query.Kind(query.Relationship(), ad.Contains),
			query.Kind(query.End(), ad.Container),
			query.Equals(query.EndProperty(common.ObjectID.String()), "AB616901-D423-4B9A-B68F-D24CEE1E36EF")),

		//// CONTAINERS
		query.And(
			query.Kind(query.Start(), ad.Group),
			query.Equals(query.StartProperty(common.ObjectID.String()), "S-1-5-21-3130019616-2776909439-2417379446-512"),
			query.Kind(query.Relationship(), ad.Owns),
			query.Kind(query.End(), ad.Container),
			query.Equals(query.EndProperty(common.ObjectID.String()), "AB616901-D423-4B9A-B68F-D24CEE1E36EF")),
		query.And(
			query.Kind(query.Start(), ad.Container),
			query.Equals(query.StartProperty(common.ObjectID.String()), "AB616901-D423-4B9A-B68F-D24CEE1E36EF"),
			query.Kind(query.Relationship(), ad.Contains),
			query.Kind(query.End(), ad.Computer),
			query.Equals(query.EndProperty(common.ObjectID.String()), "S-1-5-21-3130019616-2776909439-2417379446-2120")),

		//// COMPUTERS
		query.And(
			query.Kind(query.Start(), ad.User),
			query.Equals(query.StartProperty(common.ObjectID.String()), "S-1-5-21-3130019616-2776909439-2417379446-1105"),
			query.Kind(query.Relationship(), ad.AdminTo),
			query.Kind(query.End(), ad.Computer),
			query.Equals(query.EndProperty(common.ObjectID.String()), "S-1-5-21-3130019616-2776909439-2417379446-1104")),
		query.And(
			query.Kind(query.Start(), ad.Group),
			query.Equals(query.StartProperty(common.ObjectID.String()), "S-1-5-21-3130019616-2776909439-2417379446-513"),
			query.Kind(query.Relationship(), ad.CanRDP),
			query.Kind(query.End(), ad.Computer),
			query.Equals(query.EndProperty(common.ObjectID.String()), "S-1-5-21-3130019616-2776909439-2417379446-1104")),
		query.And(
			query.Kind(query.Start(), ad.User),
			query.Equals(query.StartProperty(common.ObjectID.String()), "S-1-5-21-3130019616-2776909439-2417379446-1105"),
			query.Kind(query.Relationship(), ad.CanPSRemote),
			query.Kind(query.End(), ad.Computer),
			query.Equals(query.EndProperty(common.ObjectID.String()), "S-1-5-21-3130019616-2776909439-2417379446-1104")),
		query.And(
			query.Kind(query.Start(), ad.Group),
			query.Equals(query.StartProperty(common.ObjectID.String()), "TESTLAB.LOCAL-S-1-1-0"),
			query.Kind(query.Relationship(), ad.ExecuteDCOM),
			query.Kind(query.End(), ad.Computer),
			query.Equals(query.EndProperty(common.ObjectID.String()), "S-1-5-21-3130019616-2776909439-2417379446-1104")),
		query.And(
			query.Kind(query.Start(), ad.Group),
			query.Equals(query.StartProperty(common.ObjectID.String()), "TESTLAB.LOCAL-S-1-5-32-548"),
			query.Kind(query.Relationship(), ad.GenericAll),
			query.Kind(query.End(), ad.Computer),
			query.Equals(query.EndProperty(common.ObjectID.String()), "S-1-5-21-3130019616-2776909439-2417379446-1104")),
		query.And(
			query.Kind(query.Start(), ad.Computer),
			query.Equals(query.StartProperty(common.ObjectID.String()), "S-1-5-21-3130019616-2776909439-2417379446-1104"),
			query.Kind(query.Relationship(), ad.AllowedToDelegate),
			query.Kind(query.End(), ad.Computer),
			query.Equals(query.EndProperty(common.ObjectID.String()), "S-1-5-21-3130019616-2776909439-2417379446-12345")),
		query.And(
			query.Kind(query.Start(), ad.Computer),
			query.Equals(query.StartProperty(common.ObjectID.String()), "S-1-5-21-3130019616-2776909439-2417379446-12345"),
			query.Kind(query.Relationship(), ad.AllowedToAct),
			query.Kind(query.End(), ad.Computer),
			query.Equals(query.EndProperty(common.ObjectID.String()), "S-1-5-21-3130019616-2776909439-2417379446-1104")),
		query.And(
			query.Kind(query.Start(), ad.Computer),
			query.Equals(query.StartProperty(common.ObjectID.String()), "S-1-5-21-3130019616-2776909439-2417379446-1104"),
			query.Kind(query.Relationship(), ad.HasSIDHistory),
			query.Kind(query.End(), ad.User),
			query.Equals(query.EndProperty(common.ObjectID.String()), "S-1-5-21-3130019616-2776909439-2417379446-12344")),
		query.And(
			query.Kind(query.Start(), ad.Computer),
			query.Equals(query.StartProperty(common.ObjectID.String()), "S-1-5-21-3130019616-2776909439-2417379446-1104"),
			query.Kind(query.Relationship(), ad.HasSession),
			query.Kind(query.End(), ad.User),
			query.Equals(query.EndProperty(common.ObjectID.String()), "S-1-5-21-3130019616-2776909439-2417379446-1107")),
		query.And(
			query.Kind(query.Start(), ad.Computer),
			query.Equals(query.StartProperty(common.ObjectID.String()), "S-1-5-21-3130019616-2776909439-2417379446-1104"),
			query.Kind(query.Relationship(), ad.HasSession),
			query.Kind(query.End(), ad.User),
			query.Equals(query.EndProperty(common.ObjectID.String()), "S-1-5-21-3130019616-2776909439-2417379446-1108")),
		query.And(
			query.Kind(query.Start(), ad.Computer),
			query.Equals(query.StartProperty(common.ObjectID.String()), "S-1-5-21-3130019616-2776909439-2417379446-2120"),
			query.Kind(query.Relationship(), ad.CoerceToTGT),
			query.Kind(query.End(), ad.Domain),
			query.Equals(query.EndProperty(common.ObjectID.String()), "S-1-5-21-3130019616-2776909439-2417379446")),

		//// GPOs
		query.And(
			query.Kind(query.Start(), ad.Group),
			query.Equals(query.StartProperty(common.ObjectID.String()), "S-1-5-21-3130019616-2776909439-2417379446-512"),
			query.Kind(query.Relationship(), ad.GenericWrite),
			query.Kind(query.End(), ad.GPO),
			query.Equals(query.EndProperty(common.ObjectID.String()), "BE91688F-1333-45DF-93E4-4D2E8A36DE2B")),
		query.And(
			query.Kind(query.Start(), ad.Group),
			query.Equals(query.StartProperty(common.ObjectID.String()), "S-1-5-21-3130019616-2776909439-2417379446-519"),
			query.Kind(query.Relationship(), ad.WriteDACL),
			query.Kind(query.End(), ad.GPO),
			query.Equals(query.EndProperty(common.ObjectID.String()), "BE91688F-1333-45DF-93E4-4D2E8A36DE2B")),
		query.And(
			query.Kind(query.Start(), ad.Group),
			query.Equals(query.StartProperty(common.ObjectID.String()), "S-1-5-21-3130019616-2776909439-2417379446-519"),
			query.Kind(query.Relationship(), ad.WriteOwner),
			query.Kind(query.End(), ad.GPO),
			query.Equals(query.EndProperty(common.ObjectID.String()), "BE91688F-1333-45DF-93E4-4D2E8A36DE2B")),
		//// GROUPS
		query.And(
			query.Kind(query.Start(), ad.User),
			query.Equals(query.StartProperty(common.ObjectID.String()), "S-1-5-21-3130019616-2776909439-2417379446-500"),
			query.Kind(query.Relationship(), ad.MemberOf),
			query.Kind(query.End(), ad.Group),
			query.Equals(query.EndProperty(common.ObjectID.String()), "TESTLAB.LOCAL-S-1-5-32-544")),
		query.And(
			query.Kind(query.Start(), ad.Group),
			query.Equals(query.StartProperty(common.ObjectID.String()), "S-1-5-21-3130019616-2776909439-2417379446-512"),
			query.Kind(query.Relationship(), ad.Owns),
			query.Kind(query.End(), ad.Group),
			query.Equals(query.EndProperty(common.ObjectID.String()), "TESTLAB.LOCAL-S-1-5-32-544")),

		//// OUs
		query.And(
			query.Kind(query.Start(), ad.GPO),
			query.Equals(query.StartProperty(common.ObjectID.String()), "C45E9585-4932-4C03-91A8-1856869D49AF"),
			query.Kind(query.Relationship(), ad.GPLink),
			query.Kind(query.End(), ad.OU),
			query.Equals(query.EndProperty(common.ObjectID.String()), "2A374493-816A-4193-BEFD-D2F4132C6DCA")),
		query.And(
			query.Kind(query.Start(), ad.OU),
			query.Equals(query.StartProperty(common.ObjectID.String()), "2A374493-816A-4193-BEFD-D2F4132C6DCA"),
			query.Kind(query.Relationship(), ad.Contains),
			query.Kind(query.End(), ad.Computer),
			query.Equals(query.EndProperty(common.ObjectID.String()), "S-1-5-21-3130019616-2776909439-2417379446-1104")),
		query.And(
			query.Kind(query.Start(), ad.Group),
			query.Equals(query.StartProperty(common.ObjectID.String()), "S-1-5-21-3130019616-2776909439-2417379446-512"),
			query.Kind(query.Relationship(), ad.GenericAll),
			query.Kind(query.End(), ad.OU),
			query.Equals(query.EndProperty(common.ObjectID.String()), "2A374493-816A-4193-BEFD-D2F4132C6DCA")),

		//// USERS
		query.And(
			query.Kind(query.Start(), ad.Group),
			query.Equals(query.StartProperty(common.ObjectID.String()), "TESTLAB.LOCAL-S-1-5-32-544"),
			query.Kind(query.Relationship(), ad.AllExtendedRights),
			query.Kind(query.End(), ad.User),
			query.Equals(query.EndProperty(common.ObjectID.String()), "S-1-5-21-3130019616-2776909439-2417379446-1105")),
		query.And(
			query.Kind(query.Start(), ad.User),
			query.Equals(query.StartProperty(common.ObjectID.String()), "S-1-5-21-3130019616-2776909439-2417379446-1105"),
			query.Kind(query.Relationship(), ad.AllowedToDelegate),
			query.Kind(query.End(), ad.Computer),
			query.Equals(query.EndProperty(common.ObjectID.String()), "S-1-5-21-3130019616-2776909439-2417379446-12345")),
		query.And(
			query.Kind(query.Start(), ad.User),
			query.Equals(query.StartProperty(common.ObjectID.String()), "S-1-5-21-3130019616-2776909439-2417379446-1105"),
			query.Kind(query.Relationship(), ad.HasSIDHistory),
			query.Kind(query.End(), ad.User),
			query.Equals(query.EndProperty(common.ObjectID.String()), "S-1-5-21-3130019616-2776909439-2417379446-12344")),
		query.And(
			query.Kind(query.Start(), ad.User),
			query.Equals(query.StartProperty(common.ObjectID.String()), "S-1-5-21-3130019616-2776909439-2417379446-1105"),
			query.Kind(query.Relationship(), ad.SQLAdmin),
			query.Kind(query.End(), ad.Computer),
			query.Equals(query.EndProperty(common.ObjectID.String()), "S-1-5-21-3130019616-2776909439-2417379446-12345")),
		query.And(
			query.Kind(query.Start(), ad.Group),
			query.Equals(query.StartProperty(common.ObjectID.String()), "TESTLAB.LOCAL-S-1-5-32-544"),
			query.Kind(query.Relationship(), ad.AllExtendedRights),
			query.Kind(query.End(), ad.User),
			query.Equals(query.EndProperty(common.ObjectID.String()), "S-1-5-21-3130019616-2776909439-2417379446-1106")),
		query.And(
			query.Kind(query.Start(), ad.User),
			query.Equals(query.StartProperty(common.ObjectID.String()), "S-1-5-21-3130019616-2776909439-2417379446-2125"),
			query.Kind(query.Relationship(), ad.CoerceToTGT),
			query.Kind(query.End(), ad.Domain),
			query.Equals(query.EndProperty(common.ObjectID.String()), "S-1-5-21-3130019616-2776909439-2417379446")),

		//// SESSIONS
		query.And(
			query.Kind(query.Start(), ad.Computer),
			query.Equals(query.StartProperty(common.ObjectID.String()), "S-1-5-21-3130019616-2776909439-2417379446-1104"),
			query.Kind(query.Relationship(), ad.HasSession),
			query.Kind(query.End(), ad.User),
			query.Equals(query.EndProperty(common.ObjectID.String()), "S-1-5-21-3130019616-2776909439-2417379446-2117"),
			query.Equals(query.RelationshipProperty(ad.LogonType.String()), 2)),
	}
	v6ingestRelationshipAssertionCriteria = []graph.Criteria{
		query.And(
			query.Kind(query.Start(), ad.Computer),
			query.Equals(query.StartProperty(common.ObjectID.String()), "S-1-5-21-3130019616-2776909439-2417379446-1001"),
			query.Kind(query.Relationship(), ad.DCFor),
			query.Kind(query.End(), ad.Domain),
			query.Equals(query.EndProperty(common.ObjectID.String()), "S-1-5-21-3130019616-2776909439-2417379446")),
		query.And( // AbuseTGTDelegation inbound (based on external inbound)
			query.Kind(query.Start(), ad.Domain),
			query.Equals(query.StartProperty(common.ObjectID.String()), "S-1-5-21-3084884204-958224920-2707782879"),
			query.Kind(query.Relationship(), ad.AbuseTGTDelegation),
			query.Kind(query.End(), ad.Domain),
			query.Equals(query.EndProperty(common.ObjectID.String()), "S-1-5-21-3130019616-2776909439-2417379446")),
		query.And( // AbuseTGTDelegation inbound (based on Kerberos inbound)
			query.Kind(query.Start(), ad.Domain),
			query.Equals(query.StartProperty(common.ObjectID.String()), "S-1-5-21-3084884204-958224920-2707782880"),
			query.Kind(query.Relationship(), ad.AbuseTGTDelegation),
			query.Kind(query.End(), ad.Domain),
			query.Equals(query.EndProperty(common.ObjectID.String()), "S-1-5-21-3130019616-2776909439-2417379446")),
		query.And(
			query.Kind(query.Start(), ad.User),
			query.Equals(query.StartProperty(common.ObjectID.String()), "S-1-5-21-3130019616-2776909439-2417379446-1114"),
			query.Kind(query.Relationship(), ad.AdminTo),
			query.Kind(query.End(), ad.Computer),
			query.Equals(query.EndProperty(common.ObjectID.String()), "S-1-5-21-3130019616-2776909439-2417379446-1000")),
		query.And(
			query.Kind(query.Start(), ad.User),
			query.Equals(query.StartProperty(common.ObjectID.String()), "S-1-5-21-3130019616-2776909439-2417379446-2116"),
			query.Kind(query.Relationship(), ad.AdminTo),
			query.Kind(query.End(), ad.Computer),
			query.Equals(query.EndProperty(common.ObjectID.String()), "S-1-5-21-3130019616-2776909439-2417379446-1104")),
	}

	propertyAssertionCriteria = []PropertyAssertion{
		{
			ObjectID:      "TESTLAB.LOCAL-S-1-5-32-550",
			Property:      common.Name.String(),
			ExpectedValue: "PRINT OPERATORS@TESTLAB.LOCAL",
		},
		{
			ObjectID:      "S-1-5-21-3130019616-2776909439-2417379446-521",
			Property:      common.Name.String(),
			ExpectedValue: "READ-ONLY DOMAIN CONTROLLERS@TESTLAB.LOCAL",
		},
	}
)

func FormatQueryComponent(criteria graph.Criteria) string {
	var (
		emitter      = format.NewCypherEmitter(false)
		stringBuffer = &bytes.Buffer{}
	)

	if err := emitter.WriteExpression(stringBuffer, criteria.(cypher.Expression)); err != nil {
		return "ERROR"
	}

	return stringBuffer.String()
}

func IngestAssertions(testCtrl test.Controller, tx graph.Transaction) {
	for _, assertionCriteria := range ingestRelationshipAssertionCriteria {
		_, err := tx.Relationships().Filter(assertionCriteria).First()
		require.Nilf(testCtrl, err, "Unable to find an expected relationship: %s", FormatQueryComponent(assertionCriteria))
	}
}

func IngestAssertionsv6(testCtrl test.Controller, tx graph.Transaction) {
	for _, assertionCriteria := range v6ingestRelationshipAssertionCriteria {
		_, err := tx.Relationships().Filter(assertionCriteria).First()
		require.Nilf(testCtrl, err, "Unable to find an expected relationship: %s", FormatQueryComponent(assertionCriteria))
	}
}

func PropertyAssertions(testCtrl test.Controller, tx graph.Transaction) {
	for _, assertionCriteria := range propertyAssertionCriteria {
		node, err := tx.Nodes().Filterf(func() graph.Criteria {
			return query.Equals(query.NodeProperty(common.ObjectID.String()), assertionCriteria.ObjectID)
		}).First()
		require.Nilf(testCtrl, err, "Unable to find expected node %s", assertionCriteria.ObjectID)
		prop := node.Properties.Get(assertionCriteria.Property).Any()
		require.Equal(testCtrl, assertionCriteria.ExpectedValue, prop)
		require.Nilf(testCtrl, err, "Unable to find an expected relationship: %s", FormatQueryComponent(assertionCriteria))
	}
}
